package observatory

import java.io.File
import java.time.LocalDate

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

trait ExtractionTest extends FunSuite {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  test("locateTemperaturesRDD test") {

    import Extraction.sc

    val stations = sc.textFile(Extraction.filePath("/stations_sample.csv"))
    val temps = sc.textFile(Extraction.filePath("/2015_sample.csv"))
    val year = 2015

    val temperaturesYear = Extraction.locateTemperaturesSpark(year, stations, temps)
List
    val avgTemps = Extraction.locationYearlyAverageRecordsSpark(temperaturesYear)

    val expected = Array(
      (Location(0.0,0.0),23.916666666666664),
      (Location(59.792,5.341),4.541666666666667)

    )

    assert (avgTemps.collect.sameElements(expected))

//    avgTemps.collect.foreach(println)

  }

  test("empty all") {
    import Extraction.spark
    val year = 0
    val stations = spark.sparkContext.parallelize(List(""))
    val temperatures = spark.sparkContext.parallelize(List(""))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("empty stations") {
    import Extraction.spark
    val year = 0
    val stations = spark.sparkContext.parallelize(List(""))
    val temperatures = spark.sparkContext.parallelize(List("4,,01,01,32"))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("empty temperatures") {
    import Extraction.spark
    val year = 0
    val stations = spark.sparkContext.parallelize(List("4,,+1,+1"))
    val temperatures = spark.sparkContext.parallelize(List(""))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array[(LocalDate, Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("both nonempty") {
    import Extraction.spark
    val year = 0
    val stations = spark.sparkContext.parallelize(List("4,,+1,+2", "4,5,+1,+2"))
    val temperatures = spark.sparkContext.parallelize(List("4,,01,01,32", "4,10,01,01,32"))

    var computed = Extraction.locateTemperaturesSpark(year, stations, temperatures).collect()
    val expected = Array((LocalDate.of(year, 1, 1), Location(1, 2), 0))

    assert(computed.sameElements(expected))
  }

  test("average empty") {
    import Extraction.spark
    val records = spark.sparkContext.parallelize(List[(LocalDate, Location, Temperature)]())

    var computed = Extraction.locationYearlyAverageRecordsSpark(records).collect()
    val expected = Array[(Location, Temperature)]()

    assert(computed.sameElements(expected))
  }

  test("average non-empty") {
    import Extraction.spark
    val year = 4

    val records = spark.sparkContext.parallelize(
      List[(LocalDate, Location, Temperature)]
        ((LocalDate.of(year, 1, 1), Location(1, 2), 4),
          (LocalDate.of(year, 4, 1), Location(1, 2), 5),
          (LocalDate.of(year, 1, 5), Location(1, 2), 6),
          (LocalDate.of(year, 1, 1), Location(2, 2), 4)))

    var computed = Extraction.locationYearlyAverageRecordsSpark(records).collect().toSet
    val expected = Set[(Location, Temperature)]((Location(1, 2), 5), (Location(2, 2), 4))

    assert(computed == expected)
  }


}