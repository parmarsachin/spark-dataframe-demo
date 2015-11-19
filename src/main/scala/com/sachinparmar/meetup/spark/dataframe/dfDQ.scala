package com.sachinparmar.meetup.spark.dataframe

/**
 * Created by sachinparmar on 18/11/15.
 */
object dfDQ extends App {
  init.logLevel()

  val sc = init.sparkContext
  implicit val sqlContext = init.sqlContext(sc)

  implicit val dataDir = init.resourcePath

  // Read from Sample.csv attached - usecase :  taxi drop and pickup time
  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(dataDir + "Sample.csv")

  // Get Count
  println("\n count: \n" + df.count)

  // Evaluate Schema
  df.printSchema()

  // describe
  println("\n describe: \n")
  df.describe().show()

  // Data Quality

  // Elementary Data Quality checks

  //df.as("df").filter($"df.pickup_longitude" < 0).count
  println(df.filter(df("pickup_longitude") < 0).count())

  //val df1 = df.as("df").withColumn("ValidFlag", $"df.dropoff_datetime" < $"df.pickup_datetime")
  val df1 = df.withColumn("ValidFlag", df("dropoff_datetime") < df("pickup_datetime"))

  //df1.as("df").filter($"df.ValidFlag" === false).count
  println(df1.filter(df1("ValidFlag") === false).count())

  //
  df1.registerTempTable("taxi")
  sqlContext.sql("SELECT vendor_id, COUNT(*) FROM taxi GROUP BY vendor_id").show()

  // other functions
  df.distinct("rate_code")

  df.dropDuplicates(Seq("_id", "_rev")).count()

  //df.select(df("dropoff_datetime"), month(df("dropoff_datetime").cast("date"))).show()

  /*
  df.na
  drop
  fill
  replace

  na_vals.drop(Seq("passenger_count")).count
  na_vals.fill("NA", Seq("vendor_id"))

  df.freqItems
  df.stat.freqItems(Seq("vendor_id"), 0.75).show
  */
}
