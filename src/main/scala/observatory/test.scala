
import java.io.InputStream
import java.time.LocalDate

import observatory.Extraction.getClass
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

object test extends App{
  val streamStations: InputStream =
    getClass.getResourceAsStream("stations.csv")
  val stationsText: Iterator[String] =
    Source.fromInputStream(streamStations).getLines
  println(stationsText.take(3))
}
