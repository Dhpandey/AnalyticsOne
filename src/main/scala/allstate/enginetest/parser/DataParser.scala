package allstate.enginetest.parser

import utility.EmptyValueHandler

/**
  * Created by dpanw on 8/9/16.
  */

case class GPSTrip(memberDeviceID: String, tripID: String, timestamp: String, timestampEpoch: String, altitude: Double, course: Double,
                   horizontalAccuracy: Double, latitude: Double, longitude: Double, rawSpeed: Double, speedInMPH:
                   Double, verticalAccuracy: Double, timestampSpeedUnder20MPH: String, timestampTripStop: String, chargedStatus: String,
                   batterylevel: Double, ignoredStatusFlag: String, nlocations: Int)

//normal scala class ,
// because case class in scala 2.10 does not support more then 22 parameters
class MemTrip(val memberDeviceID: String, val tripID: String, val timestamp: String, val timestampEpoch: String, val accelerometerX: Double, val
accelerometerY: Double, val accelerometerZ: Double, val magnetometerX: Double, val magnetometerY: Double, val
              magnetometerZ: Double, val deviceOrientation: String, val gyroRotationX: Double, val gyroRotationY: Double, val
              gyroRotationZ: Double, val pitch: Double, val roll: Double, val yaw: Double, val rotationRateX: Double, val
              rotationRateY: Double, val rotationRateZ: Double, val activity: String, val confidence: Double, val
              relativeAltitude: String) extends Product with Serializable {

  def canEqual(that: Any) = that.isInstanceOf[MemTrip]

  def productArity = 23 // number of columns

  def productElement(idx: Int) = idx match {
    case 0 => memberDeviceID
    case 1 => tripID
    case 2 => timestamp
    case 3 => timestampEpoch
    case 4 => accelerometerX
    case 5 => accelerometerY
    case 6 => accelerometerZ
    case 7 => magnetometerX
    case 8 => magnetometerY
    case 9 => magnetometerZ
    case 10 => deviceOrientation
    case 11 => gyroRotationX
    case 12 => gyroRotationY
    case 13 => gyroRotationZ
    case 14 => pitch
    case 15 => roll
    case 16 => yaw
    case 17 => rotationRateX
    case 18 => rotationRateY
    case 19 => rotationRateZ
    case 20 => activity
    case 21 => confidence
    case 22 => relativeAltitude
  }
}


object DataParser {
  val emptyHandler = new EmptyValueHandler

  def memDataParser(memberDeviceID: String, data: String): MemTrip = {
    val row = data.split(",")
    new MemTrip(memberDeviceID, row(0), row(1), row(2), row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble, row(7).toDouble,
      row(8).toDouble, row(9), row(10).toDouble, row(11).toDouble, row(12).toDouble, row(13).toDouble, row(14).toDouble,
      row(15).toDouble, row(16).toDouble, row(17).toDouble, row(18).toDouble, row(19),
      emptyHandler.emptyNumberHandler(row(20)), emptyHandler.emptyStringHandler(row(21)))
  }

  def gpsDataParser(memberDeviceID: String, data: String): GPSTrip = {
    val row = data.split(",")
    GPSTrip(memberDeviceID, row(0), row(1), row(2), row(3).toDouble, row(4).toDouble, row(5).toDouble, row(6).toDouble,
      row(7).toDouble, row(8).toDouble, row(9).toDouble, emptyHandler.emptyNumberHandler(row(10)),
      emptyHandler.emptyStringHandler(row(11)), emptyHandler.emptyStringHandler(row(12)), row(13),
      row(14).toDouble, row(15), row(16).toInt)
  }
}
