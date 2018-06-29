package observatory

import java.time.LocalDate

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * 1st milestone: data extraction
  */
object Extraction {

  val sc: SparkContext = ???

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    locateTemperaturesRDD(year, stationsFile, temperaturesFile).collect().toSeq
  }

  def locateTemperaturesRDD(year: Year, stationsFile: String, temperaturesFile: String): RDD[(LocalDate, Location, Temperature)] = {

    //read the stations file and parse each line to a @{Station}
    val stations: RDD[Station] = sc.textFile(stationsFile) map Station.parseLine

    ???
  }

  /**Station
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}
