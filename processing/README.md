# processing/ — Traitement Spark (P2)

Couche batch (entraînement du modèle) et couche temps réel (notation des transactions) de la pipeline de détection de fraude.

```
Batch :       PaySim → HDFS → train.py (Spark MLlib) → modèle sur HDFS
Temps réel :  fichiers dans HDFS /stream/in → streaming.py (Spark Streaming) → MongoDB
```

## Environnement

| Élément | Version / détail |
|---|---|
| Cluster | Hadoop sur Docker Desktop : `hadoop-master`, `hadoop-slave1`, `hadoop-slave2` |
| Spark | 2.2.0 |
| Python (dans le master) | 3.5.2 |
| MongoDB | conteneur `mongo:5.0` sur le réseau du cluster |
| pymongo | 3.12.3 (dernière version compatible Python 3.5) |

## Fichiers

| Fichier | Rôle |
|---|---|
| `common.py` | Schéma PaySim, calcul des variables, lecture d'une ligne CSV (partagé par les deux scripts) |
| `train.py` | Entraîne le modèle Spark MLlib sur HDFS, affiche les métriques, sauvegarde le modèle sur HDFS |
| `streaming.py` | Spark Streaming (DStreams) : lit les nouveaux fichiers de `/stream/in`, applique le modèle, écrit dans MongoDB |

## Données

Télécharger **PaySim1** sur Kaggle, décompresser, renommer le CSV en `paysim.csv` et le placer dans `data/` à la racine du projet.
Le fichier n'est pas versionné (trop lourd pour GitHub, voir `.gitignore`).

- Entraînement : les 500 000 premières lignes
- Flux temps réel : les lignes 500 002 à 600 001, jamais vues par le modèle

## Où taper les commandes

| L'invite commence par | On est dans | Commandes |
|---|---|---|
| `PS C:\...>` | PowerShell (Windows) | `docker ...` |
| `root@hadoop-master:~#` | le conteneur master | `hdfs ...`, `spark-submit ...` |

Entrer dans le master : `docker exec -it hadoop-master bash` — en sortir : `exit`.

## 1. Mise en place (une seule fois)

**PowerShell**, depuis la racine du projet :

```powershell
docker start hadoop-master hadoop-slave1 hadoop-slave2
docker run -d --name mongo --network hadoop -p 27017:27017 mongo:5.0
docker cp processing hadoop-master:/root/processing
docker cp data/paysim.csv hadoop-master:/root/paysim.csv
```

> Si le conteneur `mongo` existe déjà : `docker start mongo`.
> Le nom du réseau (`hadoop`) se vérifie avec `docker network ls`.

**Dans le master** :

```bash
./start-hadoop.sh

# Spark doit utiliser Python 3 (sinon Python 2 par défaut et le code plante)
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
# Le conteneur n'a pas de locale : sans ça, Python 3.5 plante sur les accents des print
echo 'export PYTHONIOENCODING=utf-8' >> ~/.bashrc
source ~/.bashrc

# Préparation des données
head -n 500001 /root/paysim.csv > /root/paysim_train.csv
(head -1 /root/paysim.csv; sed -n '500002,600001p' /root/paysim.csv) > /root/paysim_stream.csv
hdfs dfs -mkdir -p /data /models /stream/in
hdfs dfs -put -f /root/paysim_train.csv /data/paysim.csv
```

## 2. Installer pymongo (une seule fois)

L'image ne contient pas pip. Option A, dans le master :

```bash
apt-get update
apt-get install -y python3-pip
pip3 install "pymongo==3.12.3"
```

Option B, si apt-get échoue — télécharger depuis Windows (**PowerShell**) :

```powershell
pip download pymongo==3.12.3 --python-version 3.5 --platform manylinux1_x86_64 --only-binary=:all: -d wheels
docker cp wheels hadoop-master:/root/wheels
```

puis l'installer sans pip (**dans le master**) :

```bash
cd /root/wheels
python3 -c "import zipfile,glob,site; zipfile.ZipFile(glob.glob('pymongo-*.whl')[0]).extractall(site.getsitepackages()[0])"
python3 -c "import pymongo; print(pymongo.version)"     # doit afficher 3.12.3
```

## 3. Entraîner le modèle

**Dans le master** :

```bash
cd /root/processing
spark-submit --master local[2] --py-files common.py train.py \
  --data hdfs:///data/paysim.csv --model hdfs:///models/fraud_model
```

Affiche la matrice de confusion, precision, recall, F1, AUC-PR, puis `Modèle sauvegardé dans hdfs:///models/fraud_model`.

Options : `--algo lr` (régression logistique pondérée au lieu de la Random Forest), `--neg-ratio 10` (normales gardées par fraude pour la Random Forest).

## 4. Lancer le temps réel

**Terminal 1 (master)** — laisser tourner :

```bash
cd /root/processing
spark-submit --master local[2] --py-files common.py streaming.py \
  --input hdfs:///stream/in --model hdfs:///models/fraud_model --mongo mongodb://mongo:27017
```

**Terminal 2 (master)** — mini-simulateur de test :

```bash
for i in 1 2 3 4 5; do
  (head -1 /root/paysim_stream.csv; sed -n "$((i*1000)),$((i*1000+999))p" /root/paysim_stream.csv) > /root/batch_$i.csv
  hdfs dfs -put -f /root/batch_$i.csv /stream/tmp_$i.csv
  hdfs dfs -mv /stream/tmp_$i.csv /stream/in/batch_$i.csv
  echo "fichier $i envoyé"; sleep 6
done
```

Le terminal 1 affiche `[date] 1000 transactions notées, X alertes` toutes les ~5 secondes.

**Vérification (PowerShell)** :

```powershell
docker exec -it mongo mongosh fraud --eval "db.transactions.countDocuments()"
docker exec -it mongo mongosh fraud --eval "db.transactions.findOne({prediction: 1})"
```

## 5. Repartir de zéro

```bash
hdfs dfs -rm -r -f /stream/in/*                                        # master
```
```powershell
docker exec -it mongo mongosh fraud --eval "db.transactions.drop()"    # PowerShell
```

## Format écrit dans MongoDB (contrat avec P3)

Base `fraud`, collection `transactions` :

```json
{ "step": 1, "type": "TRANSFER", "amount": 181.0,
  "nameOrig": "C1305486145", "nameDest": "C553264065",
  "isFraud": 1, "prediction": 1, "probability": 0.94,
  "processed_at": "2026-11-02T10:15:03" }
```

## Problèmes rencontrés et solutions

| Problème | Cause | Solution |
|---|---|---|
| `docker` inconnu dans le master | les commandes docker se tapent sous Windows | `exit`, puis PowerShell |
| `pip3` inconnu | l'image ne contient pas pip | section 2 (apt-get ou wheel depuis Windows) |
| Erreur de syntaxe ou d'encodage Python au lancement | Spark utilise Python 2 par défaut | `export PYSPARK_PYTHON=python3` |
| `UnicodeEncodeError: 'ascii' codec can't encode character` | le conteneur n'a pas de locale (`LANG` vide), Python 3.5 écrit en ASCII | `export PYTHONIOENCODING=utf-8` |
| `ImportError: No module named 'numpy'` | `pyspark.ml` a besoin de numpy, absent de l'image | `apt-get install -y python3-numpy` |
| `foreachBatch` introuvable | n'existe qu'à partir de Spark 2.4 | streaming réécrit en DStreams (`textFileStream` + `foreachRDD`) |
| Random Forest sans `weightCol` | non supporté en Spark 2.2 | sous-échantillonnage des normales (`--neg-ratio`) |
| Un fichier déposé n'est pas traité | Spark ignore les fichiers déjà présents au démarrage ou copiés lentement | écrire ailleurs puis `hdfs dfs -mv` vers `/stream/in`, avec un nom nouveau |

## Choix techniques

- **Variables** : montant, soldes avant/après, type encodé, et deux écarts de solde (`errorBalanceOrig`, `errorBalanceDest`) : dans une transaction normale, nouveau solde = ancien solde − montant.
- **Déséquilibre** : découpage 80/20 *avant* rééquilibrage, pour que le test garde la vraie proportion de fraudes. On évalue avec precision, recall et AUC-PR, pas l'accuracy.
- **Même préparation partout** : `common.py` et le `PipelineModel` sauvegardé garantissent que le streaming transforme les données exactement comme à l'entraînement.
