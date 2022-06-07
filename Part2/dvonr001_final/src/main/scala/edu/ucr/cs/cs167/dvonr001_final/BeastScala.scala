package edu.ucr.cs.cs167.dvonr001_final

import com.sun.org.apache.bcel.internal.generic.TABLESWITCH
import edu.ucr.cs.bdlab.beast.cg.SpatialJoinAlgorithms.{ESJDistributedAlgorithm, ESJPredicate}
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.locationtech.jts.geom.{Envelope, GeometryFactory}

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
        s"""SELECT ZIPCode, SUM(OBSERVATION_COUNT) as sum_observations
           |FROM data
           |GROUP BY ZIPCode""".stripMargin).createOrReplaceTempView("totalsum_by_zipcode")
      sparkSession.sql(                                                 //3
        s"""SELECT ZIPCode, SUM(OBSERVATION_COUNT) as sum_observations
           |FROM data
           |WHERE COMMON_NAME = "$speciesChoice"
           |GROUP BY ZIPCode""".stripMargin).createOrReplaceTempView("speciessum_by_zipcode")
      sparkSession.sql(                                                 //4 .... need to add 4th column "ratio"  (3rd column / 2nd column)
        s"""SELECT totalsum_by_zipcode.ZIPCode, totalsum_by_zipcode.sum_observations, speciessum_by_zipcode.sum_observations
          |FROM totalsum_by_zipcode
          |FULL JOIN speciessum_by_zipcode ON totalsum_by_zipcode.ZIPCode = speciessum_by_zipcode.ZIPCode""".stripMargin)


      //        .foreach(row=>println(s"${row.get(0)}\t${row.get(1)}\t${row.get(2)}"))      //for printing the  view
              .createOrReplaceTempView("species_and_tot_sum_by_zipcode")



      sparkContext.shapefile("tl_2018_us_zcta510.zip")  //7
        .toDataFrame(sparkSession)
        .createOrReplaceTempView("ZIPCODES")

      sparkSession.sql("""SELECT ZIPCode, g, ratio
            FROM species_and_tot_sum_by_zipcode, ZIPCODES
            WHERE ZIPCODE = GEOID""")                               //8   need to fix this whole query for equi-join condition
        .toSpatialRDD                                                //7
        .coalesce(1)                                    //9
        .saveAsShapefile("eBirdZIPCodeRatio")               //10




      // Load spatial datasets
      // Load a shapefile.
      // Download from: ftp://ftp2.census.gov/geo/tiger/TIGER2018/STATE/
//      val polygons = sparkContext.shapefile("tl_2018_us_state.zip")
//      // Load points in GeoJSON format.
//      // Download from https://star.cs.ucr.edu/dynamic/download.cgi/Tweets/index.geojson?mbr=-117.8538,33.2563,-116.8142,34.4099&point
//      val points = sparkContext.geojsonFile("Tweets_index.geojson")
//
//      // Run a range query
//      val geometryFactory: GeometryFactory = new GeometryFactory()
//      val range = geometryFactory.toGeometry(new Envelope(-117.337182, -117.241395, 33.622048, 33.72865))
//      val matchedPolygons: RDD[IFeature] = polygons.rangeQuery(range)
//      val matchedPoints: RDD[IFeature] = points.rangeQuery(range)
//
//      // Run a spatial join operation between points and polygons (point-in-polygon) query
//      val sjResults: RDD[(IFeature, IFeature)] =
//        matchedPolygons.spatialJoin(matchedPoints, ESJPredicate.Contains, ESJDistributedAlgorithm.PBSM)
//
//      // Keep point coordinate, text, and state name
//      val finalResults: RDD[IFeature] = sjResults.map(pip => {
//        val polygon: IFeature = pip._1
//        val point: IFeature = pip._2
//        val values = point.toSeq :+ polygon.getAs[String]("NAME")
//        val schema = StructType(point.schema :+ StructField("state", StringType))
//        new Feature(values.toArray, schema)
//      })
//
//      // Write the output in CSV format
//      finalResults.saveAsCSVPoints(filename = "output", xColumn = 0, yColumn = 1, delimiter = ';')
    } finally {
      sparkSession.stop()
    }
  }
}