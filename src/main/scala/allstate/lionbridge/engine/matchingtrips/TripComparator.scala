package allstate.lionbridge.engine.matchingtrips

import java.text.SimpleDateFormat

/**
  * Created by dpanw on 9/15/16.
  */
class TripComparator extends java.io.Serializable {

  def startLocationFilter(tripStartLocation1: String, tripStartLocation2: String) = {
    val startCoordinates1 = tripStartLocation1.split(",")
    val startLatitude1 = startCoordinates1(0).toDouble
    val startLongitude1 = startCoordinates1(1).toDouble
    val startCoordinates2 = tripStartLocation2.split(",")
    val startLatitude2 = startCoordinates2(0).toDouble
    val startLongitude2 = startCoordinates2(0).toDouble
    getDistanceFromLatLonInMeters(startLatitude1, startLongitude1, startLatitude2, startLongitude2) < ComparatorRule.LOCATION_VARIATION
  }

  def endLocationFilter(tripEndLocation1: String, tripEndLocation2: String) = {
    val endCoordinates1 = tripEndLocation1.split(",")
    val endLatitude1 = endCoordinates1(0).toDouble
    val endLongitude1 = endCoordinates1(1).toDouble
    val endCoordinates2 = tripEndLocation2.split(",")
    val endLatitude2 = endCoordinates2(0).toDouble
    val endLongitude2 = endCoordinates2(1).toDouble
    getDistanceFromLatLonInMeters(endLatitude1, endLongitude1, endLatitude2, endLongitude2) < ComparatorRule.LOCATION_VARIATION
  }

  def distanceFilter(distance1: Double, distance2: Double) = {
    Math.abs(distance1 - distance2) <= ComparatorRule.DISTANCE_VARIATION
  }

  def durationFilter(duration1: Double, duration2: Double) = {
    Math.abs(duration1 - duration2) < ComparatorRule.DURATION_VARIATION
  }

  def startTimeFilter(tripStart_TS1: String, tripStart_TS2: String) = {
    Math.abs(getEpochTime(tripStart_TS1) - getEpochTime(tripStart_TS2)) < ComparatorRule.EPOCHTIMEDURATION
  }

  def endTimeFilter(tripEnd_TS1: String, tripEnd_TS2: String) = {
    Math.abs(getEpochTime(tripEnd_TS1) - getEpochTime(tripEnd_TS2)) < ComparatorRule.EPOCHTIMEDURATION
  }

  def getEpochTime(mytime: String): Double = {
    val time = mytime.substring(0, 9) + mytime.substring(11)
    val dataFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss-SS:SS")
    val result = dataFormat.parse(time)
    result.getTime() //returns epoch time
    //mytime.split("T")(1).split(":")(0)
  }

  def getDistanceFromLatLonInMeters(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R = 6371;
    // Radius of the earth
    val latDistance = Math.toRadians(lat2 - lat1)
    val lonDistance = Math.toRadians(lon2 - lon1)
    val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
    +Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) *
      Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    R * c * 1000;
  }
}
