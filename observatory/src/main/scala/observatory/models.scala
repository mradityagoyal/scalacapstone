package observatory

import java.time.LocalDate

/**
  * Introduced in Week 1. Represents a location on the globe.
  *
  * @param lat Degrees of latitude, -90 ≤ lat ≤ 90
  * @param lon Degrees of longitude, -180 ≤ lon ≤ 180
  */
case class Location(lat: Double, lon: Double)

/**
  * Introduced in Week 3. Represents a tiled web map tile.
  * See https://en.wikipedia.org/wiki/Tiled_web_map
  * Based on http://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
  * @param x X coordinate of the tile
  * @param y Y coordinate of the tile
  * @param zoom Zoom level, 0 ≤ zoom ≤ 19
  */
case class Tile(x: Int, y: Int, zoom: Int)

/**
  * Introduced in Week 4. Represents a point on a grid composed of
  * circles of latitudes and lines of longitude.
  * @param lat Circle of latitude in degrees, -89 ≤ lat ≤ 90
  * @param lon Line of longitude in degrees, -180 ≤ lon ≤ 179
  */
case class GridLocation(lat: Int, lon: Int)

/**
  * Introduced in Week 5. Represents a point inside of a grid cell.
  * @param x X coordinate inside the cell, 0 ≤ x ≤ 1
  * @param y Y coordinate inside the cell, 0 ≤ y ≤ 1
  */
case class CellPoint(x: Double, y: Double)

/**
  * Introduced in Week 2. Represents an RGB color.
  * @param red Level of red, 0 ≤ red ≤ 255
  * @param green Level of green, 0 ≤ green ≤ 255
  * @param blue Level of blue, 0 ≤ blue ≤ 255
  */
case class Color(red: Int, green: Int, blue: Int)

case class Station(stnIdentifier: String, wbanIdentifier: Option[String], location: Option[Location])

object Station {


  def parseLine(line: String): Station = {

    //split by ,
    val split = line.split(",")
    val stnId = split.head
    val wbanId = if (split.length < 2) None else Some(split(1))
    val lat = if(split.length < 3) None else Some(split(2))
    val long = if(split.length < 4) None else Some(split(3))
    //create location.
    val location = (lat, long) match {
      case (Some(lt), Some(lng)) => Some(Location(lt.toDouble, lng.toDouble))
      case _ => None
    }
    //return Station
    Station(stnId, wbanId, location)
  }
}

case class TemperatureReading(stnId: String, wbanId: String, date: LocalDate, tempCelcious: Temperature)

object TemperatureReading {

  def parseLine(line: String, year: Int): TemperatureReading = {

    //split by ,
    val Array(stnId, wbanId, month, day, tempF) = line.split(",")
    val date =  LocalDate.of(year, month.toInt, day.toInt)
    //convert to celcious and round to 4 places
    val tempC: Double = (tempF.toDouble - 32) * 5 / 9
    val temp: Temperature = BigDecimal.valueOf(tempC).setScale(4, BigDecimal.RoundingMode.HALF_UP).toDouble
    TemperatureReading(stnId, wbanId, date, temp)
  }

}


