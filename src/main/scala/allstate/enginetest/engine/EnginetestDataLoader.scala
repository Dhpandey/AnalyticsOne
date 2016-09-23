package allstate.enginetest.engine

/**
  * Created by dpanw on 8/8/16.
  * MEM :: Micro Electro Mechanical System
  */


object EnginetestDataLoader {
  def main(args: Array[String]): Unit = {

    //Method Calls
    //new MEMDataLoader().saveFilteredData
    new TripSummaryDataLoader().saveToDatabase
  }
}



