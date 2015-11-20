package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
 * Created by sachinparmar on 16/11/15.
 */

// rdd is there
// schema is known even before reading the data
// attach schema with rdd

object dfCreate1 extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  import sqlContext.implicits._

  val dataDir = init.resourcePath

  // case class
  case class SampleSchema(id: Int, name: String, city: String)

  // df = rdd + case class
  val rdd = sc.
    textFile(dataDir + "sample-data.csv").
    map(_.split(",")).
    map(r => SampleSchema(r(0).toInt, r(1), r(2)))

  val df =  rdd.toDF("id", "name", "city")

  println("Data: ")
  df.show()
}
