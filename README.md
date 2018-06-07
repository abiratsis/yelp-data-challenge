# Yelp data challenge sample application
This is a Spark application which transform the JSON dataset found at https://www.yelp.com into tabular format. 

The application consists of:
* Spark job for the data transformation
* A docker container from which we will run the application
* Unit tests for the Spark job

## Build solution
To build solution navigate under project root directory and execute:
```
sbt compile
```

Then generate the JAR file with:
```
sbt package
```

This will create yelp-data-challenge_2.11-0.1.jar under PROJECT_DIR/target/scala-2.11

## Running Docker
To build the docker container first you will need to copy the yelp-data-challenge_2.11-0.1.jar into the PROJECT_DIR/Docker directory then from same directory execute the command:

```
docker build --rm -t yelp-data-challenge .
```
This will build a docker container named yelp-data-challenge. Next run the container with:

```
docker run -it -v PROJECT_DIR/Docker/in:/opt/spark/data/in -v PROJECT_DIR/Docker/out:/opt/spark/data/out -e YELP_DATA_PATH=/opt/spark/data/in/yelp_dataset.tar.gz yelp-data-challenge /bin/bash
```
The command accepts the following parameters:

* -it: run container in foreground mode
* -v: shared directories between host machine and docker container, here you should place the input/output data (yelp_dataset.tar.gz)
* -e: YELP_DATA_PATH is the argument which holds the data path. We will use this one through the Docker procees to extract and read the data.

## Spark job
The yelp-data-challenge_2.11-0.1.jar contains the Spark job responsible for the transormations from JSON to tabular format. The next command is part of exec_spark.sh which contains all the bash commands that should be executed as the docker entrypoint.

```
bin/spark-submit --driver-memory 4g --executor-memory 2g --class com.yelp.transformations.JsonToCsvTransformer /opt/spark/yelp-data-challenge_2.11-0.1.jar --sourceDir /tmp/dataset --outputDir /opt/spark/data/out  
```
Notice that all the results will be stored under /opt/spark/data/out folder. This is a shared directory and can be accessed through PROJECT_DIR/Docker/Data/out from the host machine.

## Running queries
To run the queries execute the next command from sbt:
```
testOnly **.JsonToCsvTransformerTest -- -z "runQueries"
```
This with run the runQueries method of the JsonToCsvTransformer class and will save the results under the /opt/spark/data/out/queries folder or PROJECT_DIR/Docker/Data/out/queries from the host machine

## Running Unit tests
Navigate under project's root directory and execute from the command line:
```
sbt
```

Then type the next command:
```
testOnly **.JsonToCsvTransformerTest -- -Dinput=file:///C:/Users/BEST_USER/Desktop/dataset -Doutput=file:///C:/Users/BEST_USER/Desktop/yelp-data-challenge/out
```

This will execute all tests under the JsonToCsvTransformerTest and takes paramteters the input/output directories in order to read/write data respectively.  

