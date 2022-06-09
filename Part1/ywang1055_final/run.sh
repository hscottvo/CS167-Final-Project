#!/usr/bin/env sh
mvn clean package

spark-submit --class edu.ucr.cs.cs167.ywang1055.Project --master "local[*]" target/ywang1055_project-1.0-SNAPSHOT.jar eBird_1k.csv eBird_1k.parquet
spark-submit --class edu.ucr.cs.cs167.ywang1055.Project --master "local[*]" target/ywang1055_project-1.0-SNAPSHOT.jar eBird_10k.csv eBird_10k.parquet
spark-submit --class edu.ucr.cs.cs167.ywang1055.Project --master "local[*]" target/ywang1055_project-1.0-SNAPSHOT.jar eBird_100k.csv eBird_100k.parquet