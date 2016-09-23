package allstate.lionbridge.engine.matchingtrips

import allstate.lionbridge.parser.TripSummaryParser.TripSummaryContainer
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.rdd.RDD
import utility.Spark

/**
  * Created by dpanw on 9/9/16.
  */
class InputComparator {

  val unionData = Spark.sparkContext.textFile("/Users/dpanw/Downloads/Data/LionBridgeData" +
    "/Lionbridge April 2016/tripsummary_QA30WLGXX0MOIDX_20160428QA30WLGXX0MOIDX61B1093.txt")
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

  def getInputTrip(): RDD[TripSummaryContainer] = {
    result
  }

}
