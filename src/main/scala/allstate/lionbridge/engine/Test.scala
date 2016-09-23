package allstate.lionbridge.engine

import java.text.SimpleDateFormat

/**
  * Created by dpanw on 9/12/16.
  */
object Test {
  def main(args: Array[String]) {
    //    println(Math.abs(67))
    //    println(Math.abs(-67))
    val d = "2016-04-28T12:23:53-06:00"
    val newTime = d.substring(0, 9) + d.substring(11)
    println(newTime)
    val dataFormat = new SimpleDateFormat("yyyy-MM-ddHH:mm:ss-SS:SS")
    val result = dataFormat.parse(newTime)
    val epoch = result.getTime()
    println(epoch)


    //    println(getDistanceFromLatLonInMiles(43.489307, -116.405951, 43.496616, -116.403983))

  }

  def getDistanceFromLatLonInMiles(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val R = 6371; // Radius of the earth

    val latDistance = Math.toRadians(lat2 - lat1)
    val lonDistance = Math.toRadians(lon2 - lon1)
    val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
    +Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2)

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
    R * c * 1000;
  }

  /*val R = 6371;
    // Radius of the earth in km
    val dLat = deg2rad(lat2 - lat1);
    // deg2rad below
    val dLon = deg2rad(lon2 - lon1);
    val a = Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(lat2)) *
        Math.sin(dLon / 2) * Math.sin(dLon / 2);

    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
    val distance = R * c; // Distance in km
    distance * 0.621371
  }

  def deg2rad(deg: Double) = {
    deg * (Math.PI / 180)
  }*/
}
