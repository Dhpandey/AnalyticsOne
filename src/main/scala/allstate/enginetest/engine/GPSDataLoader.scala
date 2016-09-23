package allstate.enginetest.engine

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import allstate.enginetest.parser.DataParser
import utility.Spark

/**
  * Created by dpanw on 8/15/16.
  */
class GPSDataLoader extends java.io.Serializable {


  import Spark.sQLContext.implicits._

  val gpsTripData = Spark.sparkContext.textFile("/repo/Ginger/input/20160802f4ea60b64aa24f56b387ff95829f0cfd_tripInfo/" +
    "20160802f4ea60b64aa24f56b387ff95829f0cfd_GPS.txt")

  val gpsTripObjects = gpsTripData.filter(_.nonEmpty)
    .filter(row => row.split(",")(0) != "tripID").filter(row => row.split(",")(0).length >= 30)
    .map(row => DataParser.gpsDataParser(row.split(",")(0).substring(8, 23), row))

  def saveToMongoDB() = {
    val saveConfig = MongodbConfigBuilder(
      Map(Host -> List("localhost:27017"),
        Database -> "EngineTestDB",
        Collection -> "GPSData",
        SamplingRatio -> 1.0,
        WriteConcern -> "normal",
        SplitSize -> 8,
        SplitKey -> "_id"))

    gpsTripObjects.toDF().saveToMongodb(saveConfig.build())
  }

  def saveToFile() = {
    gpsTripObjects.toDF.coalesce(1).write.option("header", "true").format("csv").save("/repo/Ginger/output")
  }
}

object MainContainer {
  def main(args: Array[String]) {
    val gPSDataLoader = new GPSDataLoader()
    gPSDataLoader.saveToFile()
  }
}
