gcloud dataproc clusters create pyspark-practice \
  --enable-component-gateway \
  --region us-west1 \
  --zone us-west1-a \
  --no-address \
  --num-workers 1 \
  --master-machine-type n1-standard-2 \
  --master-boot-disk-type pd-balanced \
  --master-boot-disk-size 400 \
  --image-version 2.2-debian12 \
  --optional-components JUPYTER \
  --project centi-data-engineering

gcloud dataproc jobs submit pyspark \
  /home/ilmancino/PycharmProjects/spark-pyspark-practice/python/main.py \
  --cluster=pyspark-practice \
  --region=us-west1
