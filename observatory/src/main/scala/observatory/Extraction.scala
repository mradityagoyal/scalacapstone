package observatory

import java.io.File
import java.time.LocalDate

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 1st milestone: data extraction
  */
object Extraction {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  private val log = LogManager.getRootLogger
  log.setLevel(Level.WARN)


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Observatory")
      .config("spark.master", "local")
      .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    locateTemperaturesSpark(year, sc.textFile(filePath(stationsFile)), sc.textFile(filePath(temperaturesFile))).collect().toSeq
  }

  /**
    * @param year             Year number
    * @param stations    RDD of stations file line
    * @param temperatures RDD Of temp file lines.
    * @return A RDD containing triplets (date, location, temperature)
    */
  def locateTemperaturesSpark(year: Year, stations: RDD[String], temperatures: RDD[String]): RDD[(LocalDate, Location, Temperature)] = {

    //Station.csv
    //STN identifier	WBAN identifier	Latitude	Longitude
    //read the stations file and parse each line to a @{Station}
    val keydLocations: RDD[((String, String), Location)] = stations.filter(!_.isEmpty).map(_.split(","))
      .filter(_.length == 4)
      .map{
        case Array(stnId, wabnId, lat, long) => ((stnId, wabnId), Location(lat.toDouble, long.toDouble))
      }

    //Temperatures
    //STN identifier	WBAN identifier	Month	Day	Temperature (in degrees Fahrenheit)
    val keydTemp: RDD[((String, String), (String, String, LocalDate, Temperature))] = temperatures.filter(!_.isEmpty)
      .map(_.split(","))
      .filter(_.length == 5)
      .map {
        case Array(stnId, wbanId, month, day, tempF) =>
          val date = LocalDate.of(year, month.toInt, day.toInt)
          //convert to celcious and round to 4 places
          val tempC: Double = (tempF.toDouble - 32) * 5 / 9
//          val temp: Temperature = BigDecimal.valueOf(tempC).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
          val temp: Temperature = tempC
          (stnId, wbanId, date, temp)
      }.keyBy(t => (t._1, t._2))

    val joined: RDD[((String,String), (Location, (String, String, LocalDate, Temperature)))] = keydLocations.join(keydTemp)
    joined.map {
      case (id, (loc, (stId, wabnId, date, temp))) => (date, loc, temp)
    }
  }

  /**
    *
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    locationYearlyAverageRecordsSpark(sc.parallelize(records.toSeq)).collect.toSeq
  }


  /**
    *
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecordsSpark(records: RDD[(LocalDate, Location, Temperature)]): RDD[(Location, Temperature)] = {
    records.map{
      case (_, loc, temp) => (loc , (temp, 1L))
    }.reduceByKey{
      case ((t1, c1), (t2, c2)) => (t1 + t2, c1+c2)
    }.map{
      case (loc, (tempSum, count)) =>
//        val avgTemp = BigDecimal.valueOf(tempSum / count).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
        val avgTemp = tempSum / count
        (loc, avgTemp)
    }
  }


  def filePath(fileName: String) = {
    val resource = this.getClass.getClassLoader.getResource(fileName.tail)
    if (resource == null) sys.error("Please download the dataset as explained in the assignment instructions")
    new File(resource.toURI).getPath
  }


}
