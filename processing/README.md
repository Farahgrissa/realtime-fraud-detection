# processing/ — traitement Spark (P2)
Télécharger PaySim sur Kaggle et le placer dans data/paysim.csv voila le lien : https://www.kaggle.com/datasets/mtalaltariq/paysim-data


| Fichier | Rôle |
|---|---|
| `common.py` | Schéma PaySim + calcul des variables, partagé par les deux scripts |
| `train.py` | Couche batch : entraîne le modèle MLlib sur HDFS et le sauvegarde |
| `streaming.py` | Couche temps réel : lit `/stream/in`, note chaque transaction, écrit dans MongoDB |
| `../tools/fake_paysim.py` | Faux CSV au format PaySim pour tester sans le vrai jeu de données |

## Lancer

```bash
pip install pymongo        

# 1. Entraînement (--sample 0.2 pour aller plus vite sur le vrai PaySim)
spark-submit --py-files common.py train.py \
  --data hdfs:///data/paysim.csv --model hdfs:///models/fraud_model

# 2. Streaming
spark-submit --py-files common.py streaming.py \
  --input hdfs:///stream/in --model hdfs:///models/fraud_model \
  --checkpoint hdfs:///stream/checkpoint --mongo mongodb://<hôte-mongo>:27017
```

En local sans HDFS : remplacer les chemins `hdfs:///...` par des dossiers normaux.

## Le modèle

- Variables : montant, soldes avant/après, type (encodé), et deux écarts de solde
  (`errorBalanceOrig`, `errorBalanceDest`) : dans une transaction normale, nouveau solde = ancien solde − montant.
- Déséquilibre : chaque fraude pèse `nb_normales / nb_fraudes` (colonne `weight`).
- `--algo rf` (Random Forest, par défaut) ou `--algo lr` (régression logistique).
  Le poids sur Random Forest demande Spark 3.0 ou plus : sur Spark 2.x, utiliser `--algo lr`.
- On regarde precision, recall et AUC-PR, pas l'accuracy (99,9 % en prédisant toujours « non fraude »).

## Format écrit dans MongoDB 

Base `fraud`, collection `transactions` :
`step, type, amount, nameOrig, nameDest, isFraud, prediction, probability, processed_at`

## P1

Le simulateur doit écrire chaque fichier ailleurs puis le **déplacer** dans `/stream/in`
(`hdfs dfs -mv`), sinon Spark peut lire un fichier à moitié écrit.
Chaque fichier doit garder la ligne d'en-tête du CSV.
