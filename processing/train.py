import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from common import PAYSIM_SCHEMA, NUMERIC_FEATURES, add_features


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--data", default="hdfs:///data/paysim.csv")
    p.add_argument("--model", default="hdfs:///models/fraud_model")
    p.add_argument("--algo", choices=["rf", "lr"], default="rf",
                   help="rf = Random Forest, lr = régression logistique")
    p.add_argument("--sample", type=float, default=1.0,
                   help="fraction des données à utiliser (ex. 0.1 pour aller vite)")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("fraud-train").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Lecture depuis HDFS
    df = spark.read.csv(args.data, header=True, schema=PAYSIM_SCHEMA)
    if args.sample < 1.0:
        df = df.sample(fraction=args.sample, seed=42)
    df = add_features(df).na.drop()

    # 2. Déséquilibre des classes : les fraudes sont très rares (~0,1 % dans PaySim).
    #    On donne à chaque fraude un poids = nb_normales / nb_fraudes,
    #    sinon le modèle prédit "non fraude" partout et a quand même 99,9 % d'accuracy.
    counts = {r["isFraud"]: r["count"] for r in df.groupBy("isFraud").count().collect()}
    n_ok, n_fraud = counts.get(0, 0), counts.get(1, 0)
    if n_fraud == 0:
        raise SystemExit("Aucune fraude dans les données : vérifier le fichier.")
    ratio = n_ok / n_fraud
    print("Transactions : %d normales, %d fraudes (poids fraude = %.1f)" % (n_ok, n_fraud, ratio))
    df = df.withColumn("weight", F.when(F.col("isFraud") == 1, ratio).otherwise(1.0))

    train, test = df.randomSplit([0.8, 0.2], seed=42)

    # 3. Pipeline : encodage du type -> assemblage des variables -> classifieur.
    #    Tout est sauvegardé ensemble, donc le streaming applique exactement les mêmes étapes.
    indexer = StringIndexer(inputCol="type", outputCol="type_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["type_idx"], outputCols=["type_vec"])
    assembler = VectorAssembler(inputCols=NUMERIC_FEATURES + ["type_vec"], outputCol="features")
    if args.algo == "rf":
        clf = RandomForestClassifier(labelCol="isFraud", featuresCol="features",
                                     weightCol="weight", numTrees=50, maxDepth=8, seed=42)
    else:
        clf = LogisticRegression(labelCol="isFraud", featuresCol="features",
                                 weightCol="weight", maxIter=50)
    model = Pipeline(stages=[indexer, encoder, assembler, clf]).fit(train)

    # 4. Évaluation sur le jeu de test
    pred = model.transform(test)
    auc_pr = BinaryClassificationEvaluator(labelCol="isFraud", metricName="areaUnderPR",
                                           rawPredictionCol="rawPrediction").evaluate(pred)
    c = (pred.groupBy("isFraud", "prediction").count().collect())
    m = {(int(r["isFraud"]), int(r["prediction"])): r["count"] for r in c}
    tp, fp, fn = m.get((1, 1), 0), m.get((0, 1), 0), m.get((1, 0), 0)
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    print("\n=== Résultats sur le jeu de test (%s) ===" % args.algo)
    print("Fraudes détectées : %d / %d" % (tp, tp + fn))
    print("Fausses alertes   : %d" % fp)
    print("Precision : %.3f | Recall : %.3f | F1 : %.3f | AUC-PR : %.3f"
          % (precision, recall, f1, auc_pr))

    # 5. Sauvegarde du modèle (sur HDFS en mode cluster)
    model.write().overwrite().save(args.model)
    print("\nModèle sauvegardé dans", args.model)
    spark.stop()


if __name__ == "__main__":
    main()
