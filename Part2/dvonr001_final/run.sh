mvn clean package
spark-submit --class edu.ucr.cs.cs167.dvonr001_final.BeastScala --master "local[*]" target/dvonr001_final-1.0-SNAPSHOT.jar file:///'pwd'/eBird_ZIP_10k.parquet "Mallard"
