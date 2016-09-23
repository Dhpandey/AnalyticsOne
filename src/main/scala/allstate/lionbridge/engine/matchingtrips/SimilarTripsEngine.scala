package allstate.lionbridge.engine.matchingtrips

import allstate.lionbridge.parser.TripSummaryParser.TripSummaryContainer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import utility.Spark

/**
  * Created by dpanw on 9/9/16.
    */
class SimilarTripsEngine extends java.io.Serializable {

  val customComparator = new TripComparator

  val lionbridge_march_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge March 2016/tripsummary*.txt")
  val lionbridge_april_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge April 2016/tripsummary*.txt")
  val lionbridge_may_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge May 2016/tripsummary*.txt")
  val lionbridge_june_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge June 2016/tripsummary*.txt")

  val tripSummary_rdds = Seq(lionbridge_april_2016, lionbridge_may_2016, lionbridge_june_2016, lionbridge_march_2016)
  val unionData = Spark.sparkContext.union(tripSummary_rdds)

  val result = unionData.mapPartitions(record => {
    val jacksonMapper = new ObjectMapper()
    jacksonMapper.registerModule(DefaultScalaModule)
    record.flatMap(record => {
      try {
        Some(jacksonMapper.readValue(record, classOf[TripSummaryContainer]))
      } catch {
        case e: Exception => None
      }
    })
  }, true)

  def getSimilarTrips(trip: TripSummaryContainer) = {

    val result1 = result.filter(data => customComparator.startLocationFilter(data.tripSummaryUpload.productElement(24).toString,
      trip.tripSummaryUpload.productElement(24).toString))
    val result2 = result1.filter(data => customComparator.endLocationFilter(data.tripSummaryUpload.productElement(43).toString,
      trip.tripSummaryUpload.productElement(43).toString))
    val result3 = result2.filter(data => customComparator.distanceFilter(data.tripSummaryUpload.productElement(31).toString.toDouble,
      trip.tripSummaryUpload.productElement(31).toString.toDouble))
    val result4 = result3.filter(data => customComparator.durationFilter(data.tripSummaryUpload.productElement(41).toString.toDouble,
      trip.tripSummaryUpload.productElement(41).toString.toDouble))
    val result5 = result4.filter(data => customComparator.startTimeFilter(data.tripSummaryUpload.productElement(7).toString,
      trip.tripSummaryUpload.productElement(7).toString))
    val result6 = result5.filter(data => customComparator.endTimeFilter(data.tripSummaryUpload.productElement(54).toString,
      trip.tripSummaryUpload.productElement(54).toString))
     result6
  }

  def getTripsWithMatchingStartLocation(trip: TripSummaryContainer) = {
    result
      .filter(data => customComparator.startLocationFilter(data.tripSummaryUpload.productElement(24).toString,
      trip.tripSummaryUpload.productElement(24).toString))
  }

  def getTripsWithMatchingEndLocation(trip: TripSummaryContainer) = {
    result.filter(data => customComparator.endLocationFilter(data.tripSummaryUpload.productElement(43).toString,
      trip.tripSummaryUpload.productElement(43).toString))
  }

  def getTripsWithMatchingDistance(trip: TripSummaryContainer) = {
    result.filter(data => customComparator.distanceFilter(data.tripSummaryUpload.productElement(31).toString.toDouble,
      trip.tripSummaryUpload.productElement(31).toString.toDouble))
  }

  def getTripsWithMatchingDuration(trip: TripSummaryContainer) = {
    result.filter(data => customComparator.durationFilter(data.tripSummaryUpload.productElement(41).toString.toDouble,
      trip.tripSummaryUpload.productElement(41).toString.toDouble))
  }

  def getTripsWithMatchingStartTime(trip: TripSummaryContainer) = {
    result.filter(data => customComparator.startTimeFilter(data.tripSummaryUpload.productElement(7).toString,
      trip.tripSummaryUpload.productElement(7).toString))
  }

  def getTripsWithMatchingEndTime(trip: TripSummaryContainer) = {
    result.filter(data => customComparator.endTimeFilter(data.tripSummaryUpload.productElement(54).toString,
      trip.tripSummaryUpload.productElement(54).toString))
  }

  def getTripWithMultipleCriteria(trip: TripSummaryContainer) = {

    result.filter(data => customComparator.startLocationFilter(data.tripSummaryUpload.productElement(24).toString,
      trip.tripSummaryUpload.productElement(24).toString))
      .filter(data => customComparator.endLocationFilter(data.tripSummaryUpload.productElement(43).toString,
        trip.tripSummaryUpload.productElement(43).toString))
      .filter(data => customComparator.distanceFilter(data.tripSummaryUpload.productElement(31).toString.toDouble,
        trip.tripSummaryUpload.productElement(31).toString.toDouble))
      .filter(data => customComparator.durationFilter(data.tripSummaryUpload.productElement(41).toString.toDouble,
        trip.tripSummaryUpload.productElement(41).toString.toDouble))
      .filter(data => customComparator.startTimeFilter(data.tripSummaryUpload.productElement(7).toString,
        trip.tripSummaryUpload.productElement(7).toString))
      .filter(data => customComparator.endTimeFilter(data.tripSummaryUpload.productElement(54).toString,
        trip.tripSummaryUpload.productElement(54).toString))
  }
}

object SimilarTripsEngine {

  import Spark.sQLContext.implicits._

  def main(args: Array[String]) {
    //similar trips
    val input = new InputComparator
    val engine = new SimilarTripsEngine
    val inputTrip = input.getInputTrip()

    inputTrip.toArray().foreach(x => {
      val result = engine.getSimilarTrips(x)
     // result.map(x => x.tripSummaryUpload.productElement(34)).coalesce(1).saveAsTextFile("/repo/tripID1")
      result.toDF.coalesce(1).write.json("/repo/distance2")
    })
  }
}