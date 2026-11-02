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
> - Les commandes `python ...` et `streamlit ...` se lancent là où Python est installé (Anaconda Prompt, PowerShell ou Git Bash)
> - Toutes les commandes se tapent **à la racine du projet** (le dossier qui contient `docker-compose.yml`)

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
✅ Affiche la matrice de confusion, `Precision | Recall | F1 | AUC-PR`, puis `Modèle sauvegardé dans hdfs:///models/fraud_model`.

---

## B. Démonstration (3 terminaux ouverts côte à côte)

**Terminal 1 — temps réel** (Git Bash), laisser tourner :
```bash
bash scripts/streaming.sh
```
✅ `Streaming démarré, en attente de fichiers dans hdfs:///stream/in (toutes les 5s)`

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

## D. Rejouer une démo de zéro

```bash
bash scripts/stop_streaming.sh
bash scripts/reset_stream.sh
```
Puis reprendre la partie B. Le modèle et les données dans HDFS sont conservés.

Après un redémarrage du PC ou de Docker : `docker compose up -d` puis `bash scripts/setup_master.sh` (Hadoop doit être relancé à chaque démarrage des conteneurs).

Tout arrêter : `docker compose down` (ajouter `-v` pour effacer aussi les données MongoDB).

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
│   ├── load_hdfs.sh          chargement dans HDFS
│   ├── train.sh              lance processing/train.py
│   ├── streaming.sh          lance processing/streaming.py
│   ├── stop_streaming.sh
│   └── reset_stream.sh
├── simulator/simulator.py    rejoue le flux : un petit fichier toutes les N secondes
├── processing/               Spark : train.py, streaming.py, common.py (détails dans processing/README.md)
└── dashboard/                tableau de bord Streamlit
```

## Problèmes fréquents

| Symptôme | Solution |
|---|---|
| `container name already in use` | un ancien conteneur porte le même nom : `docker rm -f <nom>` ou arrêter l'ancien cluster |
| `port is already allocated` | un autre cluster utilise 50070, 8088 ou 27017 : l'arrêter (`docker stop ...`) |
| `Live datanodes (0)` | Hadoop démarre encore : attendre 30 s et relancer `bash scripts/setup_master.sh` |
| Chemins du type `C:/Program Files/Git/...` dans une erreur | lancer les scripts avec `bash scripts/...` dans Git Bash (ils désactivent la conversion des chemins) |
| Le streaming ne voit pas les fichiers | il a été démarré après l'envoi : `reset_stream.sh`, relancer le streaming, puis le simulateur |
| Erreurs Python dans Spark (numpy, encodage, Python 2) | relancer `bash scripts/setup_master.sh` ; voir aussi `processing/README.md` |
