import argparse
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.ml import PipelineModel

from common import PAYSIM_SCHEMA, add_features, parse_line

# Champs écrits dans MongoDB : c'est le format convenu avec P3.
OUTPUT_FIELDS = ["step", "type", "amount", "nameOrig", "nameDest",
                 "isFraud", "prediction", "probability"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="hdfs:///stream/in")
    p.add_argument("--model", default="hdfs:///models/fraud_model")
    p.add_argument("--mongo", default="mongodb://mongo:27017")
    p.add_argument("--db", default="fraud")
    p.add_argument("--collection", default="transactions")
    p.add_argument("--interval", type=int, default=5, help="durée d'un micro-batch en secondes")
    return p.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("fraud-streaming").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    # Modèle entraîné par train.py (mêmes étapes de préparation que l'entraînement)
    model = PipelineModel.load(args.model)

    ssc = StreamingContext(sc, args.interval)
    lines = ssc.textFileStream(args.input)          # nouveaux fichiers du dossier HDFS

    def process(time, rdd):
        rows = rdd.map(parse_line).filter(lambda r: r is not None)
        if rows.isEmpty():
            return
        df = spark.createDataFrame(rows, PAYSIM_SCHEMA)
        scored = model.transform(add_features(df))
        result = scored.select(*OUTPUT_FIELDS).collect()

        now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        docs = []
        for r in result:
            d = r.asDict()
            d["prediction"] = int(d["prediction"])
            d["probability"] = round(float(d["probability"][1]), 4)   # probabilité de fraude
            d["processed_at"] = now
            docs.append(d)

        from pymongo import MongoClient
        client = MongoClient(args.mongo)
        client[args.db][args.collection].insert_many(docs)
        client.close()
        n_alerts = sum(d["prediction"] for d in docs)
        print("[%s] %d transactions notées, %d alertes" % (now, len(docs), n_alerts), flush=True)

    lines.foreachRDD(process)

    ssc.start()
    print("Streaming démarré, en attente de fichiers dans %s (toutes les %ds)" % (args.input, args.interval), flush=True)
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
