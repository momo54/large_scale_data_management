#!/bin/bash

## En local ->
## pig -x local -

## en dataproc...


## create the cluster
gcloud dataproc clusters create cluster-a35a --enable-component-gateway --region europe-west1 --zone europe-west1-c --master-machine-type n1-standard-4 --master-boot-disk-size 500 --num-workers 2 --worker-machine-type n1-standard-4 --worker-boot-disk-size 500 --image-version 2.0-debian10 --project master-2-large-scale-data

## copy data
gsutil cp small_page_links.nt gs://myown_bucket/

## copy pig code
gsutil cp dataproc.py gs://myown_bucket/

## Clean out directory
gsutil rm -rf gs://myown_bucket/out


## run
## (suppose that out directory is empty !!)
gcloud dataproc jobs submit pig --region europe-west1 --cluster cluster-a35a -f gs://myown_bucket/dataproc.py

## access results
gsutil cat gs://myown_bucket/out/pagerank_data_10/part-r-00000

## delete cluster...
gcloud dataproc clusters delete cluster-a35a --region europe-west1

