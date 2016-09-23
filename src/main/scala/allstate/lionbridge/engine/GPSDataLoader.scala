package allstate.lionbridge.engine

import allstate.lionbridge.parser.DataParser
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import org.apache.spark.rdd.RDD
import utility.Spark

/**
  * Created by dpanw on 8/15/16.
  */
class GPSDataLoader extends java.io.Serializable {

  import Spark.sQLContext.implicits._


  val saveConfig = MongodbConfigBuilder(
    Map(Host -> List("localhost:27017"),
      Database -> "LionbridgeDB",
      Collection -> "MemberDeviceIDSpecific",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal",
      SplitSize -> 8,
      SplitKey -> "_id"))

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

  //for single file
  //val gpsTripData = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/Lionbridge April 2016/GPS*.txt")

  //filters field headers and send each row to parser utility and gets parsed data as object
  //row.split(",")(0).length == 30) is used to filter out some inconsistency in text file
  //extract memberDeviceID from trip ID and provice to case class
  val gpsTripObjects = gpsTripData.filter(_.nonEmpty)
    .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length >= 30)
    .map(row => DataParser.gpsDataParser(row.split(",")(0).substring(8, 23), row)) //.foreach(x => println(x.timestampEpoch))

  def saveToDatabase = {
    gpsTripObjects.toDF().saveToMongodb(saveConfig.build())
  }

  //whole data related to given memberDeviceID will be aggregated
  def aggregateTripsByMemberDeviceID(memberDeviceID: String) = {
    val aggregatedTrip = gpsTripObjects.filter(trips => trips.memberDeviceID.equals(memberDeviceID))
    aggregatedTrip.toDF().coalesce(1).write.format("csv").option("header", "true").save("/repo/CSV/April/" + memberDeviceID + "Trips")
    //aggregatedTrip.toDF().saveToMongodb(saveConfig.build()
  }

  //finds distinct element
  def getDistintMemberDeviceID(): RDD[String] = {
    val data= Spark.sparkContext.textFile("")
    gpsTripObjects.map(x => x.memberDeviceID).distinct()
  }
}


