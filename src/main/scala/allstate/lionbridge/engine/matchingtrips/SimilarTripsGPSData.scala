package allstate.lionbridge.engine.matchingtrips

import allstate.lionbridge.parser.DataParser
import utility.Spark

/**
  * Created by dpanw on 9/13/16.
  */
object SimilarTripsGPSData {

  import Spark.sQLContext.implicits._

  val similarTrips = Spark.sparkContext.textFile("/repo/tripID/*")

  val lionbridge_april_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge April 2016/GPS*.txt")
  val lionbridge_may_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge May 2016/GPS*.txt")
  val lionbridge_march_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge March 2016/GPS*.txt")
  val lionbridge_june_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge June 2016/GPS*.txt")

  val GPS_rdds = Seq(lionbridge_april_2016, lionbridge_may_2016, lionbridge_march_2016, lionbridge_june_2016)
  val gpsTripData = Spark.sparkContext.union(GPS_rdds)


  val gpsTripObjects = gpsTripData.filter(_.nonEmpty)
    .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length >= 30)
    .map(row => DataParser.gpsDataParser(row.split(",")(0).substring(8, 23), row)) //.foreach(x => println(x.timestampEpoch))

  def main(args: Array[String]) {
    similarTrips.toArray().foreach(x => aggregateTripsByMemberDeviceID(x))

  }

  def aggregateTripsByMemberDeviceID(tripID: String) = {
    val aggregatedTrip = gpsTripObjects.filter(trips => trips.tripID.equals(tripID))
    aggregatedTrip.toDF().coalesce(1).write.format("csv").option("header", "true").save("/repo/SimilarTrips/" + tripID + "Trips")
  }
}
