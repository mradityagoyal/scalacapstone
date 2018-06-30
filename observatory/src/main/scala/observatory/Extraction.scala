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

    //Station.csv
    //STN identifier	WBAN identifier	Latitude	Longitude
    //read the stations file and parse each line to a @{Station}
    //    val stations: RDD[Station] = sc.textFile(stationsFile) map Station.parseLine
    val stations: RDD[((String, String), Location)] = sc.textFile(stationsFile)
      .map(_.split(","))
      .map {
        case split => {
          val stnId = split.head
          val wbanId = if (split.length < 2) "" else split(1)
          val lat = if (split.length < 3) None else Some(split(2))
          val long = if (split.length < 4) None else Some(split(3))
          //create location.
          val location = (lat, long) match {
            case (Some(lt), Some(lng)) => Some(Location(lt.toDouble, lng.toDouble))
            case _ => None
          }

          ((stnId, wbanId), location)
        }
      }.filter(_._2 != None)
      .map {
        case (id, maybeLoc) => (id, maybeLoc.get)
      }

    val keyed: RDD[((String, String), ((String, String), Location))] = stations.keyBy(_._1)



    //Temperatures
    //STN identifier	WBAN identifier	Month	Day	Temperature (in degrees Fahrenheit)
    val temperatures: RDD[((String, String), LocalDate, Temperature)] = sc.textFile(temperaturesFile)
        .map{
          case line => {
            val Array(stnId, wbanId, month, day, tempF) = line.split(",")
            val date =  LocalDate.of(year, month.toInt, day.toInt)
            //convert to celcious and round to 4 places
            val tempC: Double = (tempF.toDouble - 32) * 5 / 9
            val temp: Temperature = BigDecimal.valueOf(tempC).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
            ((stnId, wbanId), date, temp)
          }
        }

    stations

    ???
  }

  /** Station
    *
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    ???
  }

}
