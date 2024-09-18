#!/bin/bash

curl -o moby_dick.txt https://www.gutenberg.org/files/2701/2701-0.txt

# Variables à personnaliser
PROJECT_ID="sobike44"
BUCKET_NAME="expmoby"
CLUSTER_NAME="wordcount-cluster"
REGION="us-central1"  # Ou la région que vous préférez
INPUT_FILE_PATH="moby_dick.txt"
OUTPUT_PATH="output"
ZONE="us-central1-a"  # Ou la zone que vous préférez
WORDCOUNT_SCRIPT="wordcount.py"

# Activer les APIs nécessaires
gcloud services enable dataproc.googleapis.com
gcloud services enable storage.googleapis.com

# Configurer le projet par défaut
gcloud config set project $PROJECT_ID

# Créer un bucket GCS (si nécessaire)
gsutil mb -l $REGION gs://$BUCKET_NAME/

# Copier le fichier d'entrée dans le bucket
gsutil cp $INPUT_FILE_PATH gs://$BUCKET_NAME/

# Créer un script PySpark WordCount
cat << EOF > $WORDCOUNT_SCRIPT
from pyspark import SparkContext

sc = SparkContext("local", "Word Count")

# Lire le fichier d'entrée
text_file = sc.textFile("gs://$BUCKET_NAME/$INPUT_FILE_PATH")

# Faire le WordCount
counts = (text_file.flatMap(lambda line: line.split(" "))
          .map(lambda word: (word, 1))
          .reduceByKey(lambda a, b: a + b))

# Sauvegarder le résultat dans Google Cloud Storage
counts.saveAsTextFile("gs://$BUCKET_NAME/$OUTPUT_PATH")
EOF

# Créer un cluster DataProc minimal (single-node)
gcloud dataproc clusters create $CLUSTER_NAME \
    --region=$REGION \
    --zone=$ZONE \
    --single-node \
    --master-machine-type=n1-standard-2 \
    --master-boot-disk-size=50GB \
    --image-version=2.0-debian10

# Soumettre le job PySpark au cluster
gcloud dataproc jobs submit pyspark $WORDCOUNT_SCRIPT \
    --cluster=$CLUSTER_NAME \
    --region=$REGION

# Attendre la fin du job (sinon le script continue)
sleep 10

# Récupérer le résultat depuis GCS
gsutil cat gs://$BUCKET_NAME/$OUTPUT_PATH/part-00000

# Supprimer le cluster après exécution
gcloud dataproc clusters delete $CLUSTER_NAME --region=$REGION --quiet

# Supprimer les fichiers générés
#rm -f $WORDCOUNT_SCRIPT
