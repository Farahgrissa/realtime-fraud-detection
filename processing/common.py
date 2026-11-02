"""Éléments partagés entre train.py et streaming.py."""
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, IntegerType,
                               StringType, DoubleType)

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
NUMERIC_FEATURES = [
    "amount",
    "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "errorBalanceOrig", "errorBalanceDest",
]


def add_features(df):
    """Dans une transaction normale : nouveau solde = ancien solde - montant.
    Un écart non nul est un signal fort de fraude dans PaySim.
    """
    return (df
            .withColumn("errorBalanceOrig",
                        F.col("newbalanceOrig") + F.col("amount") - F.col("oldbalanceOrg"))
            .withColumn("errorBalanceDest",
                        F.col("oldbalanceDest") + F.col("amount") - F.col("newbalanceDest")))
