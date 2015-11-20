package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 18/11/15.
 */
object dfDQ extends App {
  init.logLevel()

  val sc = init.sparkContext
  implicit val sqlContext = init.sqlContext(sc)
  import sqlContext.implicits._

  implicit val dataDir = init.resourcePath

  // data
  val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(dataDir + "Sample.csv")

  // Get Count
  println("\n count: \n" + df.count)

  // Schema - first level of defense
  df.printSchema()

  // describe
  println("\n describe: \n")
  df.describe().show()

  // Data Quality

  // null values

  //println("null checks...")

  //df.selectExpr("length(store_and_fwd_flag)").show(25)


  println("drop count: "+df.na.drop("all", Seq("store_and_fwd_flag")).count())

  // Elementary Data Quality checks

  println("elementry check...."+ df.filter(df("pickup_longitude") < 0).count())

  val df1 = df.withColumn("ValidFlag", df("dropoff_datetime") < df("pickup_datetime"))
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
