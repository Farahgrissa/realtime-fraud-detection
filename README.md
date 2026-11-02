# Détection de fraude en temps réel — Projet Big Data (ENSI, II3)

Chaque transaction de paiement mobile est notée par un modèle de machine learning au moment où elle arrive,
et les fraudes probables s'affichent en direct sur un tableau de bord.

```
Batch :       PaySim → HDFS → Spark MLlib (entraînement) → modèle sauvegardé dans HDFS
Temps réel :  simulateur → HDFS /stream/in → Spark Streaming (notation) → MongoDB → tableau de bord Streamlit
```

| Rôle | Membre | Dossier |
|---|---|---|
| P1 — Infrastructure et collecte | Ghaith Sayari | `docker-compose.yml`, `scripts/`, `simulator/` |
| P2 — Traitement Spark | Farah Grissa | `processing/` |
| P3 — NoSQL et visualisation | Sahar Elloumi | `dashboard/` |

---

## Prérequis

- **Docker Desktop** démarré, avec au moins 4 Go de RAM (Settings → Resources)
- **Git for Windows** (fournit le terminal **Git Bash**)
- **Python 3** avec `pandas`, `pymongo` et `streamlit` (Anaconda convient) : `pip install pandas pymongo streamlit`
- Le jeu de données **PaySim1** (Kaggle, `ealaxi/paysim1`) : décompresser, renommer le CSV en **`data/paysim.csv`**

> **Quel terminal ?**
> - Les scripts `.sh` se lancent dans **Git Bash** : `bash scripts/...`
> - **Attention dans PowerShell** : `bash` y lance WSL et non Git Bash (erreur `execvpe(/bin/bash) failed`).
>   Utiliser le chemin complet, par exemple avec un raccourci :
>   ```powershell
>   Set-Alias gbash "C:\Program Files\Git\bin\bash.exe"
>   gbash scripts/setup_master.sh
>   ```
> - Les commandes `python ...` et `streamlit ...` se lancent là où Python est installé (Anaconda Prompt, PowerShell ou Git Bash)
> - Toutes les commandes se tapent **à la racine du projet** (le dossier qui contient `docker-compose.yml`), sur la branche **`main`**

---

## A. Préparation (une seule fois, environ 20 minutes)

**1. Récupérer le projet**
```bash
git clone https://github.com/Farahgrissa/realtime-fraud-detection.git
cd realtime-fraud-detection
```
Placer le CSV de PaySim dans `data/paysim.csv`.

**2. Démarrer les conteneurs** (cluster Hadoop : 1 master, 2 slaves, plus MongoDB)
```bash
docker compose up -d
docker ps
```
✅ 4 conteneurs : `fraud-hadoop-master`, `fraud-hadoop-slave1`, `fraud-hadoop-slave2`, `fraud-mongo`.

**3. Installer ce qui manque dans le master et démarrer Hadoop** (Git Bash)
```bash
bash scripts/setup_master.sh
```
✅ Se termine par `Live datanodes (2)` et `OK : environnement prêt`.

**4. Découper les données** (Python)
```bash
python scripts/split_data.py
```
✅ Crée trois fichiers dans `data/` :
- `paysim_train.csv` : 500 000 transactions pour l'entraînement
- `paysim_stream.csv` : 20 % de PaySim, **jamais vues** par le modèle, rejouées par le simulateur
- `paysim_stream_demo.csv` : les fraudes de ce flux plus 20 000 transactions normales, pour que la démo montre des détections

**5. Charger l'entraînement dans HDFS** (Git Bash)
```bash
bash scripts/load_hdfs.sh
```
✅ `/data/paysim_train.csv` apparaît dans HDFS.

**6. Entraîner le modèle** (Git Bash, quelques minutes)
```bash
bash scripts/train.sh --no-error-features --neg-ratio 50
```
✅ Affiche les variables utilisées, la matrice de confusion, `Precision | Recall | F1 | AUC-PR` (environ 0,88 / 0,71 / 0,79 / 0,79), puis `Modèle sauvegardé dans hdfs:///models/fraud_model`.

Options :
- `--no-error-features` : sans les écarts de solde, en partie liés au simulateur (voir « Résultats du modèle »)
- `--neg-ratio 50` : 50 transactions normales gardées par fraude à l'entraînement (10 par défaut)
- `--model hdfs:///models/<nom>` : sauvegarder ailleurs pour comparer, sans écraser le modèle de la démo

---

## B. (3 terminaux ouverts côte à côte)

**Terminal 1 — temps réel** (Git Bash), laisser tourner :
```bash
bash scripts/streaming.sh
```
✅ `Streaming démarré, en attente de fichiers dans hdfs:///stream/in (toutes les 5s)`

<details>
<summary>Commande complète lancée par le script</summary>

```bash
docker exec fraud-hadoop-master bash -c "export PYSPARK_PYTHON=python3 PYTHONIOENCODING=utf-8; \
  cd /root/processing && spark-submit --master local[2] --py-files common.py streaming.py \
  --input hdfs:///stream/in --model hdfs:///models/fraud_model --mongo mongodb://mongo:27017"
```
Pas d'option `--checkpoint` : le streaming en DStreams n'en a pas besoin.
</details>

**Terminal 2 — tableau de bord** (Python), laisser tourner :
```bash
streamlit run dashboard/dashboard.py
```
✅ Le navigateur s'ouvre sur http://localhost:8501

**Terminal 3 — envoi des transactions** (Python) :
```bash
python simulator/simulator.py --source data/paysim_stream_demo.csv --rows 200 --interval 2
```
✅ Terminal 3 : `tx_..._000001.csv envoyé (200 transactions, N fraudes)` toutes les 2 secondes
✅ Terminal 1 : `[date] ... transactions notées, X alertes` toutes les 5 secondes
✅ Tableau de bord : les compteurs montent et les alertes apparaissent

Arrêter le simulateur avec **Ctrl+C**, puis le temps réel :
```bash
bash scripts/stop_streaming.sh
```

> Le fichier de démo est volontairement enrichi en fraudes (environ 7 % au lieu de 0,13 %) pour qu'elles soient visibles en quelques minutes.
> Ces fraudes ne font pas partie des données d'entraînement. Pour un flux réaliste : `--source data/paysim_stream.csv`.

---

## C. Vérifier

| Quoi | Commande ou adresse |
|---|---|
| HDFS (interface web) | http://localhost:50070 → Utilities → Browse the file system → `/stream/in` |
| YARN | http://localhost:8088 |
| Tableau de bord | http://localhost:8501 |
| Nombre de transactions dans MongoDB | `docker exec fraud-mongo mongo fraud --quiet --eval "db.transactions.countDocuments({})"` |
| Une alerte | `docker exec fraud-mongo mongo fraud --quiet --eval "printjson(db.transactions.findOne({prediction: 1}))"` |
| Matrice de confusion sur le flux (réel × prédit) | voir ci-dessous |

Prédictions comparées aux vraies étiquettes sur les transactions reçues en temps réel :

```powershell
# PowerShell (les accents graves protègent les $)
docker exec fraud-mongo mongo fraud --quiet --eval "db.transactions.aggregate([{`$group: {_id: {reel: '`$isFraud', predit: '`$prediction'}, n: {`$sum: 1}}}]).forEach(printjson)"
```
```bash
# Git Bash
docker exec fraud-mongo mongo fraud --quiet --eval 'db.transactions.aggregate([{$group: {_id: {reel: "$isFraud", predit: "$prediction"}, n: {$sum: 1}}}]).forEach(printjson)'
```



## Résultats du modèle

Random Forest (Spark MLlib), 500 000 transactions d'entraînement (645 fraudes), évaluation sur 20 % tenus à l'écart :

| Modèle | Precision | Recall | F1 | AUC-PR | Fraudes manquées | Fausses alertes |
|---|---|---|---|---|---|---|
| Avec écarts de solde, ratio 10 | 1,000 | 1,000 | 1,000 | 1,000 | 0 / 121 | 0 |
| Sans écarts de solde, ratio 10 | 0,274 | 0,810 | 0,409 | 0,786 | 23 / 121 | 260 |
| **Sans écarts de solde, ratio 50 (modèle retenu)** | **0,878** | **0,711** | **0,785** | **0,789** | 35 / 121 | **12** |

- **Le score parfait vient des écarts de solde** (`errorBalanceOrig`, `errorBalanceDest`) : sans elles, avec les mêmes données et le même modèle, la precision tombe de 1,00 à 0,27. PaySim est simulé, et une fraude y vide le compte d'une façon que les transactions normales ne reproduisent presque jamais. Un arbre de décision coupe sur une variable à la fois et ne sait pas recalculer seul une différence entre soldes.
- **Le modèle retenu exclut ces variables**, en partie liées au simulateur, pour se placer dans un cadre plus réaliste.
- **Le ratio règle le compromis precision / recall** : l'AUC-PR ne change presque pas (0,786 → 0,789), mais garder 50 normales par fraude au lieu de 10 fait passer les fausses alertes de 260 à 12, au prix de quelques fraudes manquées en plus.
- Le jeu de test est découpé **avant** le rééquilibrage et garde la vraie proportion de fraudes. Le flux (`paysim_stream.csv`) ne contient aucune transaction de l'entraînement.

Pour reproduire la comparaison :
```bash
bash scripts/train.sh --model hdfs:///models/fraud_model_complet
bash scripts/train.sh --no-error-features --model hdfs:///models/fraud_model_sans_ecarts
bash scripts/train.sh --no-error-features --neg-ratio 50
```

---

## Format des documents MongoDB

Base `fraud`, collection `transactions`, un document par transaction notée :

```json
{ "step": 1, "type": "TRANSFER", "amount": 181.0,
  "nameOrig": "C1305486145", "nameDest": "C553264065",
  "isFraud": 1, "prediction": 1, "probability": 0.94,
  "processed_at": "2026-11-02T10:15:03" }
```

| Champ | Signification |
|---|---|
| `isFraud` | vrai label PaySim (pour vérifier le modèle) |
| `prediction` | décision du modèle (1 = fraude) |
| `probability` | probabilité de fraude estimée |
| `processed_at` | heure de traitement par Spark |

Depuis Windows (tableau de bord), MongoDB est à `mongodb://localhost:27017` ; depuis le cluster, à `mongodb://mongo:27017`.

---

## Organisation du dépôt

```
├── docker-compose.yml        cluster Hadoop (1 master, 2 slaves) + MongoDB
├── data/                     PaySim et fichiers découpés (non versionnés)
├── scripts/
│   ├── setup_master.sh       pip, numpy, pymongo, variables Python, démarrage de Hadoop
│   ├── split_data.py         découpage entraînement / flux / démo, sans recouvrement
│   ├── check_results.py      vérifications : recouvrement, séparabilité, règle de référence
│   ├── load_hdfs.sh          chargement dans HDFS
│   ├── train.sh              lance processing/train.py
│   ├── streaming.sh          lance processing/streaming.py
│   ├── stop_streaming.sh
│   └── reset_stream.sh
├── simulator/simulator.py    rejoue le flux : un petit fichier toutes les N secondes
├── processing/               Spark : train.py, streaming.py, common.py (détails dans processing/README.md)
└── dashboard/                tableau de bord Streamlit
```

