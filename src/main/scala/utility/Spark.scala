package utility

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dpanw on 8/15/16.
  */
object Spark {
  val sparkConf = new SparkConf().setAppName("MemAnalytics").setMaster("local")
  val sparkContext = new SparkContext(sparkConf)
  val sQLContext = new SQLContext(Spark.sparkContext)
}

