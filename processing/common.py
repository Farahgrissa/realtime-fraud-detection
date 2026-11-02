from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, DoubleType)

# Schéma de PaySim (colonnes du CSV d'origine, dans l'ordre).
PAYSIM_SCHEMA = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType()),
    StructField("isFlaggedFraud", IntegerType()),
])

# Variables numériques données au modèle (le type est encodé à part).
NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "errorBalanceOrig", "errorBalanceDest",
]


def add_features(df):
    return (df
            .withColumn("errorBalanceOrig",
                        F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg"))
            .withColumn("errorBalanceDest",
                        F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest")))


def parse_line(line):
    p = line.split(",")
    if len(p) != 11 or p[0] == "step":
        return None
    try:
        return (int(p[0]), p[1], float(p[2]), p[3], float(p[4]), float(p[5]),
                p[6], float(p[7]), float(p[8]), int(p[9]), int(p[10]))
    except ValueError:
        return None
