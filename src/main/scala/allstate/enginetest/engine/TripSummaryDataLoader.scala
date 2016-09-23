package allstate.enginetest.engine

import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config.MongodbConfig._
import com.stratio.datasource.mongodb.config.MongodbConfigBuilder
import utility.Spark

/**
  * Created by dpanw on 8/16/16.
  */
class TripSummaryDataLoader {

  val saveConfig = MongodbConfigBuilder(
    Map(Host -> List("localhost:27017"),
      Database -> "EngineTestDB",
      Collection -> "TripSummary",
      SamplingRatio -> 1.0,
      WriteConcern -> "normal",
      SplitSize -> 8,
      SplitKey -> "_id"))

  def saveToDatabase = {

    val dataFrames = Spark.sQLContext.read.json("/Users/dpanw/Downloads/Data" +
      "/20160802f4ea60b64aa24f56b387ff95829f0cfd_tripInfo/20160802f4ea60b64aa24f56b387ff95829f0cfd_TripSummary.json")

    dataFrames.toDF() saveToMongodb (saveConfig.build())
  }
}
