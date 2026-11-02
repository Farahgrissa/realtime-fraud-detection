# Détection de fraude en temps réel — Projet Big Data (ENSI, II3)

Chaque transaction de paiement mobile est notée par un modèle de machine learning au moment où elle arrive.
Les résultats sont stockés dans MongoDB et affichés en direct sur un tableau de bord.

| Rôle | Membre | Dossiers |
|---|---|---|
| P1 — Infrastructure et collecte | Ghaith Sayari | `docker-compose.yml`, `scripts/`, `simulator/` |
| P2 — Traitement Spark | Farah Grissa | `processing/`, `scripts/` |
| P3 — NoSQL et visualisation | Sahar Elloumi | `dashboard/`,`tools/` |

---

## Architecture

```
                         COUCHE BATCH
  PaySim (CSV) ──► HDFS /data ──► Spark MLlib ──► modèle (HDFS /models)
                                   entraînement          │
                                                         │ chargé au démarrage
                         COUCHE TEMPS RÉEL               ▼
  Simulateur ──► HDFS /stream/in ──► Spark Streaming ──► MongoDB ──► Streamlit
  (fichiers)     (dossier surveillé)   (notation)       (fraud.transactions)  (tableau de bord)
```

| Composant | Technologie | Rôle |
|---|---|---|
| Collecte | `simulator/simulator.py` | rejoue le flux de transactions : un petit fichier CSV toutes les N secondes, écrit dans `/stream/tmp` puis déplacé dans `/stream/in` (déplacement atomique) |
| Stockage distribué | HDFS (Hadoop 2.7, 1 master, 2 slaves) | historique d'entraînement, modèle, flux entrant |
| Traitement batch | Spark 2.2 MLlib | entraîne une Random Forest et sauvegarde le pipeline complet (préparation + modèle) |
| Traitement temps réel | Spark 2.2 Streaming (DStreams) | toutes les 5 s, lit les nouveaux fichiers, applique le modèle, écrit les résultats |
| Base NoSQL | MongoDB 4.4 | un document par transaction notée |
| Visualisation | Streamlit | lit MongoDB et se met à jour en direct |

Tous les services tournent dans Docker, sur un même réseau, définis dans `docker-compose.yml` :

| Conteneur | Rôle | Ports publiés |
|---|---|---|
| `fraud-hadoop-master` | NameNode, ResourceManager, Spark, code de `processing/` | 50070 (HDFS), 8088 (YARN), 4040 (Spark) |
| `fraud-hadoop-slave1`, `fraud-hadoop-slave2` | DataNodes | — |
| `fraud-mongo` | MongoDB | 27017 |

Les dossiers `data/` et `processing/` du projet sont montés dans le master (`/data` et `/root/processing`).

---

## Prérequis

- **Docker Desktop**, avec au moins 4 Go de RAM (Settings → Resources)
- **Git for Windows** (fournit **Git Bash**)
- **Python 3** avec `pandas`, `pymongo` et `streamlit` : `pip install pandas pymongo streamlit`
- Le jeu de données **PaySim1** (Kaggle, `ealaxi/paysim1`), décompressé et renommé **`data/paysim.csv`**

> **Terminaux**
> - Les scripts `.sh` se lancent dans **Git Bash** : `bash scripts/...`
> - Dans **PowerShell**, `bash` lance WSL et non Git Bash (erreur `execvpe(/bin/bash) failed`). Utiliser le chemin complet :
>   ```powershell
>   Set-Alias gbash "C:\Program Files\Git\bin\bash.exe"
>   gbash scripts/setup_master.sh
>   ```
> - Les commandes `python` et `streamlit` se lancent là où Python est installé
> - Toutes les commandes se tapent à la racine du projet

---

## Installation (une seule fois)

```bash
git clone https://github.com/Farahgrissa/realtime-fraud-detection.git
cd realtime-fraud-detection
# placer le CSV de PaySim dans data/paysim.csv

docker compose up -d                 # démarre le cluster Hadoop et MongoDB
bash scripts/setup_master.sh         # installe pip, numpy, pymongo dans le master et démarre Hadoop
python scripts/split_data.py         # découpe les données (voir ci-dessous)
bash scripts/load_hdfs.sh            # charge les données d'entraînement dans HDFS
bash scripts/train.sh --no-error-features --neg-ratio 50    # entraîne le modèle
```

`split_data.py` crée trois fichiers dans `data/`, sans aucune transaction commune entre l'entraînement et le flux :

| Fichier | Contenu |
|---|---|
| `paysim_train.csv` | 500 000 transactions pour l'entraînement (taux de fraude conservé) |
| `paysim_stream.csv` | 20 % de PaySim réservés au flux, jamais vus par le modèle |
| `paysim_stream_demo.csv` | les fraudes de ce flux plus 20 000 transactions normales (flux enrichi en fraudes) |

---

## Utilisation

Lancer les trois composants dans trois terminaux :

```bash
bash scripts/streaming.sh                          # 1. notation en temps réel (laisser tourner)
streamlit run dashboard/dashboard.py               # 2. tableau de bord : http://localhost:8501
python simulator/simulator.py                      # 3. envoi des transactions
```

Arrêter :

```bash
# Ctrl+C dans le terminal du simulateur, puis :
bash scripts/stop_streaming.sh
```

Repartir d'un flux et d'une base vides (le modèle et les données d'entraînement sont conservés) :

```bash
bash scripts/reset_stream.sh
```

Après un redémarrage de Docker : `docker compose up -d` puis `bash scripts/setup_master.sh`.
Tout arrêter : `docker compose down` (avec `-v` pour effacer aussi les données MongoDB).

---

## Référence des commandes

### `scripts/train.sh`

Lance `processing/train.py` dans le master et sauvegarde le modèle dans `hdfs:///models/fraud_model`.

| Option | Effet |
|---|---|
| `--no-error-features` | n'utilise pas les écarts de solde (`errorBalanceOrig`, `errorBalanceDest`) |
| `--neg-ratio N` | garde N transactions normales par fraude à l'entraînement (10 par défaut) |
| `--algo lr` | régression logistique pondérée au lieu de la Random Forest |
| `--model hdfs:///models/<nom>` | sauvegarde le modèle ailleurs |

### `simulator/simulator.py`

| Option | Défaut | Effet |
|---|---|---|
| `--source` | `data/paysim_stream.csv` | fichier rejoué (`data/paysim_stream_demo.csv` pour un flux enrichi en fraudes) |
| `--rows` | 200 | transactions par fichier |
| `--interval` | 2 | secondes entre deux fichiers |
| `--max-files` | 0 | nombre de fichiers à envoyer (0 = tout le fichier source) |
| `--start` | 0 | numéro du premier fichier (pour reprendre) |

Exemple : `python simulator/simulator.py --source data/paysim_stream_demo.csv --rows 200 --interval 2`

### Autres scripts

| Script | Rôle |
|---|---|
| `setup_master.sh` | dépendances Python du master, variables `PYSPARK_PYTHON` et `PYTHONIOENCODING`, démarrage de Hadoop |
| `split_data.py` | découpage entraînement / flux / flux enrichi |
| `load_hdfs.sh` | crée `/data`, `/models`, `/stream/in`, `/stream/tmp` et charge l'entraînement |
| `streaming.sh` | lance `processing/streaming.py` |
| `stop_streaming.sh` | arrête le streaming |
| `reset_stream.sh` | vide `/stream/in` et la collection MongoDB |
| `check_results.py` | vérifie le recouvrement entraînement / flux et la séparabilité des données |

<details>
<summary>Commande lancée par <code>streaming.sh</code></summary>

```bash
docker exec fraud-hadoop-master bash -c "export PYSPARK_PYTHON=python3 PYTHONIOENCODING=utf-8; \
  cd /root/processing && spark-submit --master local[2] --py-files common.py streaming.py \
  --input hdfs:///stream/in --model hdfs:///models/fraud_model --mongo mongodb://mongo:27017"
```
</details>

---

## Vérifier

| Quoi | Commande ou adresse |
|---|---|
| HDFS | http://localhost:50070 → Utilities → Browse the file system |
| YARN | http://localhost:8088 |
| Tableau de bord | http://localhost:8501 |
| Nombre de transactions notées | `docker exec fraud-mongo mongo fraud --quiet --eval "db.transactions.countDocuments({})"` |
| Une alerte | `docker exec fraud-mongo mongo fraud --quiet --eval "printjson(db.transactions.findOne({prediction: 1}))"` |

Matrice de confusion sur les transactions reçues (réel × prédit) :

```powershell
# PowerShell
docker exec fraud-mongo mongo fraud --quiet --eval "db.transactions.aggregate([{`$group: {_id: {reel: '`$isFraud', predit: '`$prediction'}, n: {`$sum: 1}}}]).forEach(printjson)"
```
```bash
# Git Bash
docker exec fraud-mongo mongo fraud --quiet --eval 'db.transactions.aggregate([{$group: {_id: {reel: "$isFraud", predit: "$prediction"}, n: {$sum: 1}}}]).forEach(printjson)'
```

---

## Format des documents MongoDB

Base `fraud`, collection `transactions` :

```json
{ "step": 1, "type": "TRANSFER", "amount": 181.0,
  "nameOrig": "C1305486145", "nameDest": "C553264065",
  "isFraud": 1, "prediction": 1, "probability": 0.94,
  "processed_at": "2026-11-02T10:15:03" }
```

| Champ | Signification |
|---|---|
| `isFraud` | vraie étiquette PaySim |
| `prediction` | décision du modèle (1 = fraude) |
| `probability` | probabilité de fraude estimée |
| `processed_at` | heure de traitement par Spark |

Adresse MongoDB : `mongodb://localhost:27017` depuis Windows, `mongodb://mongo:27017` depuis le réseau Docker.

---

## Résultats du modèle

Random Forest, 500 000 transactions d'entraînement (645 fraudes), évaluation sur 20 % tenus à l'écart (121 fraudes) :

| Modèle | Precision | Recall | F1 | AUC-PR | Fraudes manquées | Fausses alertes |
|---|---|---|---|---|---|---|
| Avec écarts de solde, ratio 10 | 1,000 | 1,000 | 1,000 | 1,000 | 0 | 0 |
| Sans écarts de solde, ratio 10 | 0,274 | 0,810 | 0,409 | 0,786 | 23 | 260 |
| **Sans écarts de solde, ratio 50 (modèle retenu)** | **0,878** | **0,711** | **0,785** | **0,789** | 35 | **12** |

- Le score parfait vient des écarts de solde : sans eux, la precision tombe de 1,00 à 0,27. PaySim est simulé, et une fraude y vide le compte d'une façon que les transactions normales ne reproduisent presque jamais.
- Le modèle retenu exclut ces variables, en partie liées au simulateur.
- Le ratio règle le compromis precision / recall : l'AUC-PR ne change presque pas, mais les fausses alertes passent de 260 à 12.

Reproduire la comparaison :

```bash
bash scripts/train.sh --model hdfs:///models/fraud_model_complet
bash scripts/train.sh --no-error-features --model hdfs:///models/fraud_model_sans_ecarts
bash scripts/train.sh --no-error-features --neg-ratio 50
```

---

## Organisation du dépôt

```
├── docker-compose.yml        cluster Hadoop (1 master, 2 slaves) + MongoDB
├── data/                     PaySim et fichiers découpés (non versionnés)
├── scripts/                  installation, découpage, lancement, vérifications
├── simulator/simulator.py    collecte : rejoue le flux dans HDFS
├── processing/               Spark : train.py, streaming.py, common.py (voir processing/README.md)
└── dashboard/                tableau de bord Streamlit
```

## Problèmes fréquents

| Symptôme | Solution |
|---|---|
| `WSL ... execvpe(/bin/bash) failed` | utiliser Git Bash, ou `& "C:\Program Files\Git\bin\bash.exe" scripts/...` dans PowerShell |
| `container name already in use` | un ancien conteneur porte le même nom : `docker rm -f <nom>` |
| `port is already allocated` | un autre programme utilise 50070, 8088 ou 27017 : l'arrêter |
| `Live datanodes (0)` | Hadoop démarre encore : attendre 30 s et relancer `bash scripts/setup_master.sh` |
| `scripts/...: No such file or directory` | se placer à la racine du projet, sur la branche `main` |
| Chemins du type `C:/Program Files/Git/...` dans une erreur | lancer les scripts avec `bash scripts/...` dans Git Bash |
| Le streaming ne traite pas les fichiers | il a été démarré après l'envoi : `reset_stream.sh`, relancer le streaming, puis le simulateur |
| Erreurs Python dans Spark (numpy, encodage, Python 2) | relancer `bash scripts/setup_master.sh` ; voir `processing/README.md` |