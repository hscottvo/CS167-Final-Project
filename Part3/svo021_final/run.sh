mvn clean package
spark-submit --class edu.ucr.cs.cs167.svo021_final.BeastScala --master "local[*]" target/svo021_final-1.0-SNAPSHOT.jar file:///../../Part1/eBird_10kZIP.parquet 02/21/2015 05/22/2015
