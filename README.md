# large scale data management

Page rank in Pig, based on https://gist.github.com/jwills/2047314
Modified for running on Google Cloud Dataproc.

## data

Data are supposed to be upload fist on Google Cloud Storage.

We need to create a root bucket to store you data and code

```
gcloud storage buckets create gs://BUCKET_NAME --project=PROJECT_ID  --location=europe-west1 --uniform-bucket-level-access
```

Next you can copy data/code using gstutils (see run.sh)


Full Data are available at: http://downloads.dbpedia.org/3.5.1/en/page_links_en.nt.bz2 (keep it compressed). 

```
bzcat page_links_en.nt.bz2 | wc -l
 119077682
```


## Running

Ensure data and code are uploaded.
Next update dataproc.py accordingly then run.

Do not forget to stop your cluster at when job is finished.

hints: 1 master / 2 slaves on page_links

```
2022-09-22 17:10:37,648 [main] INFO  org.apache.pig.Main - Pig script completed in 1 hour, 16 minutes, 49 seconds and 944 milliseconds (4609944 ms)
```

