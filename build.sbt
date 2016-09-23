name := "DrivewiseAnalytics"

version := "1.0"

scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.5.2",
  "org.apache.spark" % "spark-sql_2.10" % "1.5.2",
  "com.stratio.datasource" % "spark-mongodb_2.10" % "0.11.2",
  "org.mongodb" % "casbah-commons_2.10" % "2.8.0",
  "org.mongodb" % "casbah-core_2.10" % "2.8.0",
  "org.mongodb" % "casbah-query_2.10" % "2.8.0",
  "org.mongodb" % "mongo-java-driver" % "2.13.0",
  "com.stratio" % "spark-mongodb" % "0.8.0",
  "com.databricks" % "spark-csv_2.10" % "1.4.0",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.3",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.3.3"
)
