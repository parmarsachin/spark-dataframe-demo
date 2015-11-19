package com.sachinparmar.meetup.spark.dataframe

/**
 * Created by sachinparmar on 16/11/15.
 */

// schema is already known

object dfCreate1 extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  case class SampleSchema(id: Int, name: String, city: String)

  val rdd = sc.
    textFile(dataDir + "sample-data.csv").
    map(_.split(",")).
    map(r => SampleSchema(r(0).toInt, r(1), r(2)))

  import sqlContext.implicits._

  val df = rdd.toDF("id", "name", "city")

  //utils.showPlans(df)

  println("Schema: ")
  df.printSchema()

  println("Data: ")
  df.show()
}
