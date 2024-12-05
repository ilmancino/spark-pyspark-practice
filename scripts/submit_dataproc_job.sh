#!/bin/bash

gcloud dataproc clusters create pyspark-practice \
  --enable-component-gateway \
  --region us-west1 \
  --zone us-west1-a \
  --no-address \
  --num-workers 2 \
  --master-machine-type n1-standard-2 \
  --master-boot-disk-type pd-balanced \
  --master-boot-disk-size 400 \
  --image-version 2.2-debian12 \
  --optional-components JUPYTER \
  --project centi-data-engineering


mode="${1:-production}"

gcloud dataproc jobs submit pyspark \
  --cluster=pyspark-practice \
  --region=us-west1 \
  /home/ilmancino/PycharmProjects/spark-pyspark-practice/python/main.py -- $mode "$2"
