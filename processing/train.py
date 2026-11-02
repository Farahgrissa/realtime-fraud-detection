"""Couche batch : entraîne le modèle de détection de fraude sur l'historique PaySim.
Compatible Spark 2.2 / Python 3.5.

Exemple (dans hadoop-master) :
  spark-submit --master local[2] --py-files common.py train.py \
      --data hdfs:///data/paysim.csv --model hdfs:///models/fraud_model
"""
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
                   help="rf = Random Forest (sous-échantillonnage), lr = régression logistique (poids)")
    p.add_argument("--neg-ratio", type=float, default=10.0,
                   help="rf : nombre de transactions normales gardées par fraude à l'entraînement")
    p.add_argument("--no-error-features", action="store_true",
                   help="test : entraîner sans errorBalanceOrig ni errorBalanceDest")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("fraud-train").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Lecture depuis HDFS
    df = spark.read.csv(args.data, header=True, schema=PAYSIM_SCHEMA)
    df = add_features(df).na.drop()

    # 2. Découpage AVANT tout rééquilibrage : le jeu de test garde la vraie proportion de fraudes
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    train.cache()

    counts = {r["isFraud"]: r["count"] for r in train.groupBy("isFraud").count().collect()}
    n_ok, n_fraud = counts.get(0, 0), counts.get(1, 0)
    if n_fraud == 0:
        raise SystemExit("Aucune fraude dans les données : vérifier le fichier.")
    print("Entraînement : %d normales, %d fraudes" % (n_ok, n_fraud))

    # 3. Déséquilibre des classes (fraudes très rares)
    if args.algo == "rf":
        # La Random Forest de Spark 2.2 n'accepte pas de poids :
        # on garde toutes les fraudes et seulement une partie des transactions normales.
        frac = min(1.0, args.neg_ratio * n_fraud / float(n_ok))
        train = train.sampleBy("isFraud", fractions={0: frac, 1: 1.0}, seed=42)
        print("Sous-échantillonnage : %.4f des normales gardées (environ %d normales pour 1 fraude)"
              % (frac, int(args.neg_ratio)))
    else:
        # La régression logistique accepte un poids par ligne : chaque fraude pèse nb_normales / nb_fraudes.
        ratio = n_ok / float(n_fraud)
        train = train.withColumn("weight", F.when(F.col("isFraud") == 1, ratio).otherwise(1.0))
        print("Poids d'une fraude : %.1f" % ratio)

    # 4. Pipeline : encodage du type -> assemblage des variables -> classifieur.
    #    Tout est sauvegardé ensemble, donc le streaming applique exactement les mêmes étapes.
    indexer = StringIndexer(inputCol="type", outputCol="type_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCol="type_idx", outputCol="type_vec")
    features = NUMERIC_FEATURES
    if args.no_error_features:
        # Test d'ablation : mesurer ce que les écarts de solde apportent au modèle
        features = [c for c in NUMERIC_FEATURES if not c.startswith("errorBalance")]
    print("Variables : %s + type" % ", ".join(features))
    assembler = VectorAssembler(inputCols=features + ["type_vec"], outputCol="features")
    if args.algo == "rf":
        clf = RandomForestClassifier(labelCol="isFraud", featuresCol="features",
                                     numTrees=50, maxDepth=8, seed=42)
    else:
        clf = LogisticRegression(labelCol="isFraud", featuresCol="features",
                                 weightCol="weight", maxIter=50)
    model = Pipeline(stages=[indexer, encoder, assembler, clf]).fit(train)

    # 5. Évaluation sur le jeu de test (proportion réelle de fraudes)
    pred = model.transform(test)
    auc_pr = BinaryClassificationEvaluator(labelCol="isFraud", metricName="areaUnderPR",
                                           rawPredictionCol="rawPrediction").evaluate(pred)
    m = {}
    for r in pred.groupBy("isFraud", "prediction").count().collect():
        m[(int(r["isFraud"]), int(r["prediction"]))] = r["count"]
    tp, fp, fn, tn = m.get((1, 1), 0), m.get((0, 1), 0), m.get((1, 0), 0), m.get((0, 0), 0)
    precision = tp / float(tp + fp) if tp + fp else 0.0
    recall = tp / float(tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0

    print("\n=== Résultats sur le jeu de test (%s) ===" % args.algo)
    print("Matrice de confusion :")
    print("  fraudes détectées : %d   fraudes manquées : %d" % (tp, fn))
    print("  fausses alertes   : %d   normales correctes : %d" % (fp, tn))
    print("Precision : %.3f | Recall : %.3f | F1 : %.3f | AUC-PR : %.3f"
          % (precision, recall, f1, auc_pr))

    # 6. Sauvegarde du modèle sur HDFS
    model.write().overwrite().save(args.model)
    print("\nModèle sauvegardé dans", args.model)
    spark.stop()


if __name__ == "__main__":
    main()