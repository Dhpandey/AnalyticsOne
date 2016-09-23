package allstate.enginetest.engine

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import allstate.enginetest.parser.{DataParser, MemTrip}
import utility.Spark

/**
  * Created by dpanw on 8/15/16.
  */
class MEMDataLoader extends java.io.Serializable {

  val saveConfig = MongodbConfigBuilder(
    Map(Host -> List("localhost:27017"),
      Database -> "EngineTestDB",
      Collection -> "MEMData",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal",
      SplitSize -> 8,
      SplitKey -> "_id"))

  def splitID(trip: MemTrip): String = {
    trip.tripID.substring(8, 23)
  }

  def saveToDatabase = {
    import Spark.sQLContext.implicits._
    // val memTripData = Spark.sparkContext.textFile("hdfs://localhost:9000/user/dpanw/trips")

    val memTripData = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data" +
      "/20160802f4ea60b64aa24f56b387ff95829f0cfd_tripInfo/20160802f4ea60b64aa24f56b387ff95829f0cfd_MEM.txt")
    //filters field headers and send each row to parser utility and gets parsed data as object
    val memTripObjects = memTripData.filter(_.nonEmpty)
      .filter(row => row.split(",")(0) != "tripID")
      .filter(row => row.split(",")(0).length >= 30)
      .map(row => DataParser.memDataParser(row.split(",")(0).substring(8, 23), row)) /*.foreach(x => println(x.timestampEpoch))*/

    memTripObjects
      .toDF()
      .saveToMongodb(saveConfig.build())

  }
}
