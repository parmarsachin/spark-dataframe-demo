package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

import scala.annotation.tailrec

/**
 * Created by sachinparmar on 16/11/15.
 */


object dfDSL extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  // csv
  val csvSchema =
    StructType(
      StructField("emp_id", IntegerType, nullable = false) ::
        StructField("emp_name", StringType, nullable = true) ::
        StructField("city", StringType, nullable = true) :: Nil)

  val df = sqlContext.
    read.
    format("com.databricks.spark.csv").
    option("header", "false"). // Use first line of all files as header
    option("inferSchema", "true"). // Automatically infer data types
    schema(csvSchema).
    load(dataDir + "sample-data.csv")

  // ----------------------------------------------------------------------

  // DF DSL

  df.printSchema()
  df.show()

  df.
    select("emp_id", "city")
    .filter("emp_id > 103")
    .show()

  df.groupBy("city").count().show()

  // agg



  // sql

  df.registerTempTable("dfTable")

  val result = sqlContext.sql("select emp_id, emp_name from dfTable")

  result.show()

  // udf

  def even(value1: Int): Int = {
    val temp = Math.ceil(value1).toInt
    if (temp % 2 == 0) 1 else 0
  }

  val df1 = df.select(df("emp_id").as("id"))
  df1.select("id").show()

  // udf registration
  sqlContext.udf.register("evenUDF", even _)

  // udf with df
  df1.selectExpr("id", "evenUDF(id) as even").show()

  // udf with sql
  df1.registerTempTable("df1Table")
  val result1 = sqlContext.sql("select id, evenUDF(id) as even from df1Table")
  result1.show()
}
