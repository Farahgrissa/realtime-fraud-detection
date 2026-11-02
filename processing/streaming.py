import argparse
from datetime import datetime

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import PipelineModel

from common import PAYSIM_SCHEMA, add_features

OUTPUT_FIELDS = ["step", "type", "amount", "nameOrig", "nameDest",
                 "isFraud", "prediction", "probability"]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="hdfs:///stream/in")
    p.add_argument("--model", default="hdfs:///models/fraud_model")
    p.add_argument("--checkpoint", default="hdfs:///stream/checkpoint")
    p.add_argument("--mongo", default="mongodb://localhost:27017")
    p.add_argument("--db", default="fraud")
    p.add_argument("--collection", default="transactions")
    return p.parse_args()


def make_writer(mongo_uri, db_name, coll_name):
    """Renvoie la fonction appelée par Spark sur chaque micro-batch."""
    def write_batch(batch_df, batch_id):
        rows = batch_df.select(*OUTPUT_FIELDS).collect()   
        if not rows:
            return
        from pymongo import MongoClient
        now = datetime.now().isoformat(timespec="seconds")
        docs = []
        for r in rows:
            d = r.asDict()
            d["prediction"] = int(d["prediction"])
            d["processed_at"] = now
            docs.append(d)
        client = MongoClient(mongo_uri)
        client[db_name][coll_name].insert_many(docs)
        client.close()
        n_alerts = sum(d["prediction"] for d in docs)
        print("Batch %d : %d transactions, %d alertes" % (batch_id, len(docs), n_alerts))
    return write_batch


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("fraud-streaming").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(args.model)

    #un fichier = un micro-batch
    stream = (spark.readStream
              .schema(PAYSIM_SCHEMA)
              .option("header", True)
              .option("maxFilesPerTrigger", 1)
              .csv(args.input))


    scored = model.transform(add_features(stream).na.drop())

    fraud_proba = F.udf(lambda v: float(v[1]), DoubleType())
    scored = scored.withColumn("probability", F.round(fraud_proba("probability"), 4))

    query = (scored.writeStream
             .foreachBatch(make_writer(args.mongo, args.db, args.collection))
             .option("checkpointLocation", args.checkpoint)
             .start())
    print("Streaming démarré, en attente de fichiers dans", args.input)
    query.awaitTermination()


if __name__ == "__main__":
    main()
