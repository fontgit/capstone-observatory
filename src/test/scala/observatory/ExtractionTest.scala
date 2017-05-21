package observatory

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractionTest extends FunSuite {

  test("get resource"){
    import Extraction._
    val stationsPath = "/stations.csv"
    val tempPath = "/1980.csv"
    println(locateTemperatures(1980 ,stationsPath, tempPath))
  }

}