"""Génère un faux CSV au format PaySim, pour tester le code sans le vrai jeu de données"""
import csv, random, sys

out, n = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 50000
random.seed(0)
types = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]
with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
                "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"])
    for i in range(n):
        step = i * 744 // n + 1
        fraud = random.random() < 0.01
        t = random.choice(["TRANSFER", "CASH_OUT"]) if fraud else random.choice(types)
        old = round(random.uniform(0, 200000), 2)
        if fraud:   # compte vidé, solde destinataire non mis à jour
            amount, new = old, 0.0
            od = round(random.uniform(0, 50000), 2); nd = od
        else:
            amount = round(random.uniform(10, min(old, 50000) or 1000), 2)
            new = round(max(old - amount, 0), 2)
            od = round(random.uniform(0, 100000), 2); nd = round(od + amount, 2)
        w.writerow([step, t, amount, "C%d" % random.randint(10**8, 10**9), old, new,
                    "C%d" % random.randint(10**8, 10**9), od, nd, int(fraud), 0])
print("Écrit", n, "lignes dans", out)
