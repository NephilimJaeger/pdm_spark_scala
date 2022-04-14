package org.shape.com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_extract}


object Application {

  /*1 – Total equipment failures that happened?

    2 – Which equipment code had most failures?

    3 – Average amount of failures across equipment group, ordered by the number of failures in ascending order?
  */
  def main(args:Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    val csvSensors = spark.read.format("csv")
      .option("header","true")
      .option("sep", ";")
      .option("inferSchema", "true")
      .load("data/equipment_sensors.csv")

    val logLines = spark.read.text("data/equipment_failure_sensors.log")

    val timestampExp = "\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})]"
    val sensorExp = "\\w{6}\\[\\d+]:"

    val logsDf = logLines.select(regexp_extract(col("value"),timestampExp,1).alias("timestamp"),
                                 regexp_extract(col("value"),sensorExp,0).alias("error")
    )

    logsDf.show(3,false)




  }

}
