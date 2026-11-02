"""Insère de fausses transactions notées dans MongoDB, au même format que streaming.py.

Usage :  python seed_mongo.py [nb_transactions] [--loop]
  --loop : continue d'insérer un petit lot toutes les 3 s (simule le streaming)
"""
import random
import sys
import time
from datetime import datetime

from pymongo import MongoClient

URI, DB, COLL = "mongodb://localhost:27017", "fraud", "transactions"
TYPES = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]


def make_batch(n):
    now = datetime.now().isoformat(timespec="seconds")
    docs = []
    for _ in range(n):
        is_fraud = int(random.random() < 0.03)
        t = random.choice(["TRANSFER", "CASH_OUT"]) if is_fraud else random.choice(TYPES)
        # modèle "imparfait" : bonne détection de la plupart des fraudes + quelques fausses alertes
        if is_fraud:
            proba = random.betavariate(5, 2) if random.random() < 0.85 else random.betavariate(2, 5)
        else:
            proba = random.betavariate(1, 12) if random.random() < 0.97 else random.betavariate(5, 3)
        docs.append({
            "step": random.randint(1, 744),
            "type": t,
            "amount": round(random.uniform(10, 200000), 2),
            "nameOrig": "C%d" % random.randint(10**8, 10**9),
            "nameDest": "C%d" % random.randint(10**8, 10**9),
            "isFraud": is_fraud,
            "prediction": int(proba >= 0.5),
            "probability": round(proba, 4),
            "processed_at": now,
        })
    return docs


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 2000
    loop = "--loop" in sys.argv
    client = MongoClient(URI)
    coll = client[DB][COLL]
    coll.insert_many(make_batch(n))
    print("Inséré %d transactions dans %s.%s" % (n, DB, COLL))
    while loop:
        time.sleep(3)
        coll.insert_many(make_batch(50))
        print("+50 transactions")