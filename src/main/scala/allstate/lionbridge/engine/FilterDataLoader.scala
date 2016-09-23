package allstate.lionbridge.engine

import allstate.lionbridge.parser.DataParser
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import utility.Spark

//import com.stratio.datasource.mongodb._

/**
  * Created by dpanw on 8/22/16.
  */
class FilterDataLoader extends java.io.Serializable {

  import Spark.sQLContext.implicits._

  val saveConfig = MongodbConfigBuilder(
    Map(Host -> List("10.148.135.121:27017"),
      Database -> "LionbridgeDB",
      Collection -> "FilterData",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal",
      SplitSize -> 8,
      SplitKey -> "_id"))

  def saveToDatabase() = {
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


    //filters field headers and send each row to parser utility and gets parsed data as object
    //row.split(",")(0).length == 30) is used to filter out some inconsistency in text file
    //extract memberDeviceID from trip ID and provice to case class
    val gpsTripObjects = gpsTripData.filter(_.nonEmpty)
      .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length >= 30)
      .map(row => DataParser.gpsDataParser(row.split(",")(0).substring(8, 23), row)) //.foreach(x => println(x.timestampEpoch))

    val maxLatitude = 42.17853247
    val minLatitude = 42.10134555
    val maxLongtitude = -87.9000
    val minLongtitude = -88.3000
    val maxTimestampEpoch: Double = 1470177717000L
    val minTimestampEpoch: Double = 1459617562.000132

    val filteredData = gpsTripObjects
      .filter(data => data.latitude <= maxLatitude && data.latitude >= minLatitude)
      .filter(data => data.longitude <= maxLongtitude && data.longitude >= minLongtitude)
    //.filter(data => data.timestampEpoch.toDouble < maxTimestampEpoch && data.timestampEpoch.toDouble > minTimestampEpoch)
    //.foreach(data => println(data.timestampEpoch))

    filteredData.coalesce(1).toDF.write.json("/opt/filteredData")
    // saves to mongodb
    //filteredData.toDF.saveToMongodb(saveConfig.build())
  }

}