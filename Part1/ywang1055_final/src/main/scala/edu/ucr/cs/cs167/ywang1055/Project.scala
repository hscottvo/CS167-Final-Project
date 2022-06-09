package edu.ucr.cs.cs167.ywang1055

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object Project {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Project Part 1")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputFile: String = args(0)
    val outputFile: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()

      val dataframe = sparkSession.read.format("csv")
        .option("sep", ",")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputFile)
      dataframe.selectExpr("*", "ST_CreatePoint(x, y) AS geometry")
      val reducedDF = dataframe.select("x", "y", "GLOBAL UNIQUE IDENTIFIER", "CATEGORY", "COMMON NAME", "SCIENTIFIC NAME", "SUBSPECIES COMMON NAME", "OBSERVATION COUNT", "OBSERVATION DATE")
      val renamedDF = reducedDF.withColumnRenamed("GLOBAL UNIQUE IDENTIFIER","GLOBAL_UNIQUE_IDENTIFIER")
               .withColumnRenamed("COMMON NAME", "COMMON_NAME")
               .withColumnRenamed("SCIENTIFIC NAME", "SCIENTIFIC_NAME")
               .withColumnRenamed("SUBSPECIES COMMON NAME", "SUBSPECIES_COMMON_NAME")
               .withColumnRenamed("OBSERVATION COUNT", "OBSERVATION_COUNT")
               .withColumnRenamed("OBSERVATION DATE", "OBSERVATION_DATE")
      val dataframeRRD: SpatialRDD = renamedDF.selectExpr("*", "ST_CreatePoint(x, y) AS geometry").toSpatialRDD
      val zipRDD: SpatialRDD = sparkContext.shapefile("tl_2018_us_zcta510.zip")
      val dfZipRRD: RDD[(IFeature, IFeature)] = dataframeRRD.spatialJoin(zipRDD)
      val dfZip: DataFrame = dfZipRRD.map({ case (data, zip) => Feature.append(data, zip.getAs[String]("ZCTA5CE10"), "ZIPCode") })
        .toDataFrame(sparkSession)
      val dfZipDropGeo = dfZip.drop("geometry")
      dfZipDropGeo.printSchema()
      dfZipDropGeo.show()
      dfZipDropGeo.write.mode(SaveMode.Overwrite).parquet(outputFile)

      val t2 = System.nanoTime()
      println(s"Operation on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
    } finally {
      sparkSession.stop()
    }
  }
}
