package observatory

import org.apache.commons.lang3.StringUtils
import org.scalatest.FunSuite

import scala.io.Source

trait ModelTest extends FunSuite {

  test("Temperature reading is parsed"){

    val input = """010010,,01,04,15.9
                   010010,,01,05,21.4
                   010010,,01,06,17.9
                   010010,,01,07,9.0
                   010010,,01,08,20.1
                   010010,,01,09,23.2
                   010010,,01,10,9.1
                   010010,,01,11,7.2
                   010010,,01,12,24.5
                   010010,,01,13,22.2
                   010010,,01,14,20.9
                   010010,,01,15,8.8
                   010010,,01,16,26.9
                   010010,,01,17,19.7
                   010010,,01,18,11.0
                   010010,,01,19,17.6
                   010010,,01,20,9.1
                   010010,,01,21,20.0
                   010010,,01,22,22.3
                   010010,,01,23,16.6
                   010010,,01,24,9.1
                   010010,,01,25,8.0
                   010010,,01,26,8.6
                   010010,,01,27,8.0
                   010010,,01,28,20.0"""

    val lines = input.lines.map(StringUtils.strip)

    val tempReadings = lines.map(TemperatureReading.parseLine(_, 1975))

    //check that the stantionids hash code matches precalculated expected.
    assert(tempReadings.map(_.stnId).mkString.hashCode === 1163784352)

  }


  test("Stations are parsed") {

    val input = """007005,,,
                  |007011,,,
                  |007018,,+00.000,+000.000
                  |007025,,,
                  |007026,,+00.000,+000.000
                  |007034,,,
                  |007037,,,
                  |007044,,,"""


    val lines = input.lines.map(StringUtils.strip)

    val staions = lines.map(Station.parseLine)

    assert(staions.map(_.stnIdentifier).mkString.hashCode == -1461486356)
  }

  
}