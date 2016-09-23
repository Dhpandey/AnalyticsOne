package allstate.lionbridge.engine

/**
  * Created by dpanw on 8/8/16.
  * MEM :: Micro Electro Mechanical System
  */


object LionbridgeDataLoader {
  def main(args: Array[String]): Unit = {

    //Method Calls
    //  new MEMDataLoader().saveToMongoDB
    // new TripSummaryDataLoader().saveToMongoDB

    //    val trips = new TripSummaryDataLoader().getDistintMemberDeviceID()
    //    trips.toArray().foreach(x => new TripSummaryDataLoader().aggregateTripsByMemberDeviceID(x))

    //new FilterDataLoader().saveToMongoDB()
    //new GPSDataLoader().aggregateTripsByMemberDeviceID("QA16Q8WXTFD0A8J")

    //gets all distinct memberDeviceID and saves data for each creating seprate files
    //val gps = new GPSDataLoader().getDistintMemberDeviceID()
    // gps.toArray().foreach(x => new GPSDataLoader().aggregateTripsByMemberDeviceID(x))


  }
}



