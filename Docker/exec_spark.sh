#!/bin/sh
set -e

echo "Extracting ${YELP_DATA_PATH} ..."

INPUTDIR=$(dirname "${YELP_DATA_PATH}")

cd $INPUTDIR

echo "Moving yelp_dataset.tar.gz to /tmp ..."
mv yelp_dataset.tar.gz /tmp
cd /tmp/

echo "Running gunzip yelp_dataset.tar.gz ..."
gunzip yelp_dataset.tar.gz

echo "Running tar -xvf yelp_dataset.tar ..."
tar -xvf yelp_dataset.tar

echo "Contents of /tmp/dataset:"
ls -la /tmp/dataset

echo "Cleaning up /opt/spark/data/out/ dir ..."
rm /opt/spark/data/out/*

cd $SPARK_HOME
echo "Executing Spark job: spark-submit --class com.yelp.transformations.YelpJsonParser /opt/spark/yelp-data-challenge_2.11-0.1.jar --sourceDir /tmp/dataset --outputDir /opt/spark/data/out"
bin/spark-submit --driver-memory 4g --executor-memory 2g --class com.yelp.transformations.JsonToCsvTransformer /opt/spark/yelp-data-challenge_2.11-0.1.jar --sourceDir /tmp/dataset --outputDir /opt/spark/data/out

echo "Cleaning up /tmp dir ..."
rm -r /tmp/dataset
rm /tmp/yelp_dataset.tar
exec "$@";