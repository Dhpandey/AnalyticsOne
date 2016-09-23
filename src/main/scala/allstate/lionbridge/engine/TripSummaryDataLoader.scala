package allstate.lionbridge.engine

import allstate.lionbridge.parser.TripSummaryParser.TripSummaryContainer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.spark.rdd.RDD
import utility.Spark

/**
  * Created by dpanw on 8/16/16.
  */
class TripSummaryDataLoader extends java.io.Serializable {

  import Spark.sQLContext.implicits._

  val saveConfig = MongodbConfigBuilder(
    Map(Host -> List("localhost:27017"),
      Database -> "LionbridgeDB",
      Collection -> "TripSummary",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal",
      SplitSize -> 8,
      SplitKey -> "_id"))

  val lionbridge_march_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge March 2016/tripsummary*.txt")
  val lionbridge_april_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge April 2016/tripsummary*.txt")
  val lionbridge_may_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge May 2016/tripsummary*.txt")
  val lionbridge_june_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
    "Lionbridge June 2016/tripsummary*.txt")

  val tripSummary_rdds = Seq(lionbridge_april_2016 /*, lionbridge_may_2016, lionbridge_june_2016, lionbridge_march_2016*/)
  val unionData = Spark.sparkContext.union(tripSummary_rdds)
  //val dataFrames = Spark.sQLContext.read.json(unionData)

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

  def saveToDatabase = {
    result.toDF.coalesce(1).write.json("/repo/tripSummary")
  }

  def aggregateTripsByMemberDeviceID(memberDeviceID: String) = {
    val aggregatedTrip = result.filter(trips => trips.tripSummaryUpload.productElement(8).equals(memberDeviceID))
    aggregatedTrip.toDF().coalesce(1).write.json("/repo/LionBridgeByMemberDeviceID/" + memberDeviceID + "Trips")
    //aggregatedTrip.toDF().saveToMongodb(saveConfig.build()
  }

  //finds distinct element
  def getDistintMemberDeviceID(): RDD[String] = {
    result.map(x => x.tripSummaryUpload.productElement(8).toString).distinct()
  }
}
