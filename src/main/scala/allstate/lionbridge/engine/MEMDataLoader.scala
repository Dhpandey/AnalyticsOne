package allstate.lionbridge.engine

import allstate.lionbridge.parser.{DataParser, MemTrip}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import utility.Spark

/**
  * Created by dpanw on 8/15/16.
  */
class MEMDataLoader extends java.io.Serializable {

  import Spark.sQLContext.implicits._

  val saveConfig = MongodbConfigBuilder(
    Map(Host -> List("localhost:27017"),
      Database -> "LionbridgeDB",
      Collection -> "MEMData",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal",
      SplitSize -> 8,
      SplitKey -> "_id"))

  def splitID(trip: MemTrip): String = {
    trip.tripID.substring(8, 23)
  }

  def saveToDatabase = {

    val lionbridge_april_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
      "Lionbridge April 2016/MEM*.txt")
    val lionbridge_may_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
      "Lionbridge May 2016/MEM*.txt")
    val lionbridge_march_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
      "Lionbridge March 2016/MEM*.txt")
    val lionbridge_june_2016 = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData/" +
      "Lionbridge June 2016/MEM*.txt")

    val MEM_rdds = Seq(lionbridge_april_2016, lionbridge_may_2016, lionbridge_march_2016, lionbridge_june_2016)
    val memTripData = Spark.sparkContext.union(MEM_rdds)

    //filters field headers and send each row to parser utility and gets parsed data as object
    val memTripObjects = memTripData.filter(_.nonEmpty)
      .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length == 30)
      .map(row => DataParser.memDataParser(row.split(",")(0).substring(8, 23), row)) /*.foreach(x => println(x.timestampEpoch))*/

    //adds deviceMemberId and saves to mongodb
    memTripObjects.toDF.saveToMongodb(saveConfig.build())


  }
}
