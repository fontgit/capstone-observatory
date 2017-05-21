package observatory

import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 1st milestone: data extraction
  */
object Extraction {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  val stationsSchema: StructType =
    StructType(List(
      StructField("STN", StringType, nullable = true),
      StructField("WBAN", StringType, nullable = true),
      StructField("Lat", DoubleType, nullable = true),
      StructField("Long", DoubleType, nullable = true)
    ))

  def read(resource: String, schema: StructType): DataFrame = {
    val rdd = spark.sparkContext.textFile(fsPath(resource)).map(_.split(",", -1)).map(Row(_))
    val dataFrame = spark.createDataFrame(rdd, schema)
    dataFrame
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(
                          year: Int,
                          stationsFile: String,
                          temperaturesFile: String): Iterable[(LocalDate, Location, Double)] = {

    val stationsLines = read(stationsFile, stationsSchema)

    Iterable((LocalDate.now(), Location(1.0, 1.0), 0.0))
  }



  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(
                                    records: Iterable[(LocalDate, Location, Double)])
  : Iterable[(Location, Double)] = {
    ???
  }

}
