# large scale data management

Page rank in Pig, based on https://gist.github.com/jwills/2047314
Modified for running on Google Cloud Dataproc

This works with private directories on Google Storage.

We need to create a root bucket to store data and code   on Google Cloud Storage

'''
gcloud storage buckets create gs://BUCKET_NAME --project=PROJECT_ID  --location=europe-west1 --uniform-bucket-level-access
'''

Data are available at: http://downloads.dbpedia.org/3.5.1/en/page_links_en.nt.bz2 (keep it compressed)
