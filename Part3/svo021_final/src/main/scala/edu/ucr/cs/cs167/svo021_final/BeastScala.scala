package edu.ucr.cs.cs167.svo021_final

import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.beast.SparkSQLRegistration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.Map

/**
 * Scala examples for Beast
 */
object BeastScala {
  def main(args: Array[String]): Unit = {
    // Initialize Spark context

    val conf = new SparkConf().setAppName("Beast Example")
    // Set Spark master to local if not already set
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")

    val spark: SparkSession.Builder = SparkSession.builder().config(conf)

    val sparkSession: SparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputFile: String = args(0)
    val startDate: String = args(1)
    val endDate: String = args(2)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._
      val t1 = System.nanoTime()
      var validOperation = true

      val df = sparkSession.read.parquet(inputFile)
//      df.printSchema()
      df.createOrReplaceTempView("data")

      sparkSession.sql(
        s"""
                SELECT COMMON_NAME, to_date(OBSERVATION_DATE, 'yyyy-MM-dd')
                FROM data
                WHERE to_date(OBSERVATION_DATE, 'yyyy-MM-dd') BETWEEN TO_DATE(CAST(UNIX_TIMESTAMP(CAST($startDate AS VARCHAR), 'MM/dd/yyyy') AS TIMESTAMP))
                AND TO_DATE(CAST(UNIX_TIMESTAMP(CAST($endDate as VARCHAR), 'MM/dd/yyyy') AS TIMESTAMP))
                LIMIT 20
                """).foreach(row => println(s"${row.get(0)}\t${row.get(1)}"))


      val t2 = System.nanoTime()
      if (validOperation)
        println(s"Operation on file '$inputFile' took ${(t2 - t1) * 1E-9} seconds")
      else
        Console.err.println(s"Invalid operation")
    } finally {
      sparkSession.stop()
    }
  }
}