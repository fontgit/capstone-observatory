package observatory

import java.io.InputStream
import java.nio.file.Paths
import java.time.LocalDate

import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

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
  import spark.implicits._

  /** @return An RDD Row compatible with the schema produced by `dfSchema`
    * @param line Raw fields
    */
  def row(line: List[String]): Row = {
    val castLine = line.head :: line.tail.map(_.toDouble)
    Row.fromSeq(castLine)
  }
  def dfSchema(columnNames: List[String]): StructType =
    StructType(
      StructField(columnNames.head, StringType, nullable = false) ::
        columnNames.tail.map { name =>
        StructField(name, DoubleType, nullable = false)
      }
    )
  def read(resource: String): (List[String], DataFrame) = {
    def fsPath(resource: String): String =
      Paths.get(getClass.getResource(resource).toURI).toString

    val rdd = spark.sparkContext.textFile(fsPath(resource))
    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)
    val data = rdd
      .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
      .map(_.split(",").to[List])
      .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

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

    val stationsLines = sc.textFile(fsPath(stationsFile))
    val temperaturesLines = sc.textFile(fsPath(temperaturesFile))

    val stations = stationsLines
      .map(_.split(",", -1))
      .filter(arr => !(arr(2).isEmpty || arr(2).isEmpty))
      .map(arr =>
        (arr.slice(0, 2).mkString(","), arr(2).toDouble, arr(3).toDouble))

    val streamTemp: InputStream =
      getClass.getResourceAsStream(temperaturesFile)

    //    val stationsText: Iterator[String] =
    //      Source.fromInputStream(streamStations).getLines
    //    val tempText: Iterator[String] =
    //      Source.fromInputStream(streamTemp).getLines

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
