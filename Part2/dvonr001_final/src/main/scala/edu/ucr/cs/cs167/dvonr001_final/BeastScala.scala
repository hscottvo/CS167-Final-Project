package edu.ucr.cs.cs167.dvonr001_final

import com.sun.org.apache.bcel.internal.generic.TABLESWITCH
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Envelope, GeometryFactory}
import org.apache.spark.beast.SparkSQLRegistration


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

    val sparkSession = spark.getOrCreate()
    val sparkContext = sparkSession.sparkContext
    SparkSQLRegistration.registerUDT
    SparkSQLRegistration.registerUDF(sparkSession)

    val inputFile: String = args(0)
    val speciesChoice: String = args(1)
    try {
      // Import Beast features
      import edu.ucr.cs.bdlab.beast._

      val df = sparkSession.read.parquet(inputFile)                     //1
//      df.show
//      df.printSchema()
      df.createOrReplaceTempView("data")
      sparkSession.sql(                                                 //2
        s"""SELECT ZIPCode, SUM(OBSERVATION_COUNT) as tot_sum_observations
           |FROM data
           |GROUP BY ZIPCode""".stripMargin).createOrReplaceTempView("totalsum_by_zipcode")
      sparkSession.sql(                                                 //3
        s"""SELECT ZIPCode, SUM(OBSERVATION_COUNT) as species_sum_observations
           |FROM data
           |WHERE COMMON_NAME = "$speciesChoice"
           |GROUP BY ZIPCode""".stripMargin).createOrReplaceTempView("speciessum_by_zipcode")
      sparkSession.sql(                                                 //4 .... need to add 4th column "ratio"  (3rd column / 2nd column)
        s"""SELECT totalsum_by_zipcode.ZIPCode, (speciessum_by_zipcode.species_sum_observations / totalsum_by_zipcode.tot_sum_observations) AS ratio
          |FROM totalsum_by_zipcode
          |INNER JOIN speciessum_by_zipcode ON totalsum_by_zipcode.ZIPCode = speciessum_by_zipcode.ZIPCode""".stripMargin)
         .createOrReplaceTempView("ratio_by_zipcode")

      sparkContext.shapefile("tl_2018_us_zcta510.zip")  //7
        .toDataFrame(sparkSession)
        .createOrReplaceTempView("ZIPCODES")

      sparkSession.sql(
        s"""SELECT ZIPCode, g, ratio
           |FROM ratio_by_zipcode, ZIPCODES
           |WHERE ZIPCode = ZCTA5CE10""".stripMargin)   //8   need to fix this whole query for equi-join condition
//        .foreach(row=>println(s"${row.get(0)}\t${row.get(1)}\t${row.get(2)}"))      //for printing the view, only for testing
        .toSpatialRDD
        .coalesce(1)                                    //9
        .saveAsShapefile("eBirdZIPCodeRatio")               //10


    } finally {
      sparkSession.stop()
    }
  }
}