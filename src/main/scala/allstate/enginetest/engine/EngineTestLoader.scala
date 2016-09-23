package allstate.enginetest.engine

import utility.Spark

/**
  * Created by dpanw on 8/22/16.
  */
class EngineTestLoader {
  def saveToDatabase() = {
    val gpsTripData = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data" +
      "/20160802f4ea60b64aa24f56b387ff95829f0cfd_tripInfo/20160802f4ea60b64aa24f56b387ff95829f0cfd_GPS.txt")

    val filterData = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/TripID_Genger2_Exp.csv").filter(_.nonEmpty)
      .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length >= 30)



    val gpsTrips = gpsTripData.filter(_.nonEmpty)
      .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length >= 30)

   // gpsTrips.filter(row => row.split(",")(0) != ).saveAsTextFile("/opt/csvdata")
  }

}
