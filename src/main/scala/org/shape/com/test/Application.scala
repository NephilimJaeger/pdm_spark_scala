package org.shape.com.test

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, regexp_extract, regexp_replace}


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

    val equipments = spark.read.format("json")
      .option("inferSchema", "true")
      .option("multiline","true")
      .load("data/equipment.json")

    val timeExp = "\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})]"
    val errorExp = "\\w{6}\\[\\d+]:"
    val sensorExp = "\\d+"
    val dateEXP = "\\d{4}-\\d{2}-\\d{2}"

    val logsDf = logLines.select(regexp_extract(col("value"),timeExp,1).alias("time"),
                                 regexp_extract(col("value"),errorExp,0).alias("error"))
    .withColumn("sensor_id", regexp_extract(col("error"),sensorExp,0))
    .withColumn("date", regexp_extract(col("time"),dateEXP, 0))
    .drop("error")

    val januaryFailures = logsDf.filter(col("date").between("2020-01-01", "2020-01-31"))

  /* 1 – Total equipment failures that happened in January 2020?
    11645
  */

   val dfEquipments = csvSensors.join(januaryFailures, Seq("sensor_id"))
                                .join(equipments, Seq("equipment_id"))

   val countEquipmentFailures = dfEquipments.groupBy("code").count().orderBy(col("count").desc)

    /* 2 – Which equipment code had most failures?
     Equipment with code E1AD07D4 had 1377 failures in January. Curiously the equipment with code 2C195700 was the only one with no failures at all.
     */


    val dfGroupOfEquipments = dfEquipments.groupBy("group_name","code").count()
    val dfAvgFailures = dfGroupOfEquipments.groupBy("group_name").agg(avg("count").alias("avg_failures"))


    dfAvgFailures.orderBy(col("avg_failures")).show(false)
















}

}
