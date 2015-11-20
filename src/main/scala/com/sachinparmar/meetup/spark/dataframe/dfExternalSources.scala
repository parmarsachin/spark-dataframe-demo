package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
 * Created by sachinparmar on 16/11/15.
 */


object dfExternalSources extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  // ------------------------------------------------------------------------------

  // csv
  val csvSchema =
    StructType(
      StructField("emp_id", IntegerType, nullable = false) ::
        StructField("emp_name", StringType, nullable = true) ::
        StructField("city", StringType, nullable = true) :: Nil)

  val csvDF = sqlContext.
    read.
    format("com.databricks.spark.csv").
    option("header", "false"). // Use first line of all files as header
    option("inferSchema", "false"). // Automatically infer data types
    schema(csvSchema).
    load(dataDir + "sample-data.csv")

  println("Schema (csvDF): ")
  csvDF.printSchema()
  println("Data (csvDF): ")
  csvDF.show()

  // ------------------------------------------------------------------------------

  // json
  val jsonDF = sqlContext.
    read.
    json(dataDir + "emp.json")

  println("Schema (jsonDF): ")
  jsonDF.printSchema()
  println("Data (jsonDF): ")
  jsonDF.show()

  // save as parquet
  jsonDF.
    write.
    mode(SaveMode.Overwrite).
    format("parquet").
    save(dataDir + "emp.parquet")

  // ------------------------------------------------------------------------------

  val parquetDF = sqlContext.
    read.
    parquet(dataDir + "emp.parquet")

  println("Schema (parquetDF): ")
  parquetDF.printSchema()
  println("Data (parquetDF): ")
  parquetDF.show()
}
