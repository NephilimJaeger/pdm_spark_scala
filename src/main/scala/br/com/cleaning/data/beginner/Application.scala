package br.com.cleaning.data.beginner

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, regexp_extract}


object Application {

  def main(args:Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Building a SparkSession at the local machine
    val spark = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    // Loading the data
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

    // Regular Expressions necessary to extract the data from the log archive
    val timeExp = "\\[(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})]"
    val errorExp = "\\w{6}\\[\\d+]:"
    val sensorExp = "\\d+"
    val dateEXP = "\\d{4}-\\d{2}-\\d{2}"


    val logsDf = logLines.select(regexp_extract(col("value"),timeExp,1).alias("time"),
                                 regexp_extract(col("value"),errorExp,0).alias("error"))
    .withColumn("sensor_id", regexp_extract(col("error"),sensorExp,0))
    .withColumn("date", regexp_extract(col("time"),dateEXP, 0))
    .drop("error")

    // Filtering out the data from January 2020
    val januaryFailures = logsDf.filter(col("date").between("2020-01-01", "2020-01-31"))

    // Dataframe with all the data about the equipments
   val dfEquipments = csvSensors.join(januaryFailures, Seq("sensor_id"))
                                .join(equipments, Seq("equipment_id"))

    // Grouping the equipments by the code to count which one presents the most failures
   val countEquipmentFailures = dfEquipments.groupBy("code").count().orderBy(col("count").desc)

   // Counting the errors by equipment group and code
    val dfGroupOfEquipments = dfEquipments.groupBy("group_name","code").count()

    // Average of the errors by group of equipments, ordered by the number of failures
    val dfAvgFailures = dfGroupOfEquipments.groupBy("group_name").agg(avg("count").alias("avg_failures"))
      .orderBy(col("avg_failures"))




















}

}
