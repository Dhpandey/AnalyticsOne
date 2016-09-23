package utility

/**
  * Created by dpanw on 8/22/16.
  */


object JsonToCSV {
  def main(args: Array[String]) {
    convert()
  }

  //  def convert() = {
  //    val df = Spark.sQLContext.read.json("/repo/Kevin/Jsonschemachange")
  //    val selecteddata = df.select(
  //      "tripId", "memberDeviceId", "date", "brakingCount", "distance", "startTime", "endTime", "duration",
  //      "speedingCount", "tripStartLocation", "tripEndLocation", "tripStart_TS", "tripEnd_TS", "tripTerminateId",
  //      "tripTerminateReason", "geoPointCount")
  //    selecteddata.coalesce(1).write
  //      .format("csv")
  //      .option("header", "true")
  //      // .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
  //      .save("/repo/Kevin/Csvschemachange")
  //  }

  def convert() = {
    val df = Spark.sQLContext.read.json("/repo/JSON/QA1FV0IBE7NXG1XTrips")
    df.coalesce(1).write
      .format("csv")
      .option("header", "true")
      // .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("/repo/CSV/QA1FV0IBE7NXG1XTrips")
  }
}
