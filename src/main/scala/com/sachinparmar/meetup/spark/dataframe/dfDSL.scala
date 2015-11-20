package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
 * Created by sachinparmar on 16/11/15.
 */

object dfDSL extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  // ----------------------------------------------------------------------

  val dfSchema =
    StructType(
      StructField("emp_id", IntegerType, nullable = false) ::
        StructField("emp_name", StringType, nullable = true) ::
        StructField("salary", IntegerType, nullable = true) ::
        StructField("age", IntegerType, nullable = true) :: Nil)
  val df = sqlContext.read.schema(dfSchema).json(dataDir + "emp.json")

  println("Schema (jsonDF): ")
  df.printSchema()
  println("Data (jsonDF): ")
  df.show()

  // ----------------------------------------------------------------------

  // DF DSL
  println("simple df dsl....")
  df.
    select("emp_id", "emp_name")
    .filter("emp_id > 2")
    .show()

  df.groupBy("salary").count().show()


  // agg
  println("simple df agg....")

  df.selectExpr("avg(age) as avg_age").show()

  // sql
  println("register df as sql....")

  df.registerTempTable("dfTable")
  val result = sqlContext.sql("select emp_id, emp_name from dfTable")

  result.show()

  // udf
  println("udf in df ....")

  def even(value1: Int): Int = {
    val temp = Math.ceil(value1).toInt
    if (temp % 2 == 0) 1 else 0
  }

  // udf registration
  sqlContext.udf.register("evenUDF", even _)

  // udf with df
  df.selectExpr("id", "evenUDF(id) as even").show()

  // udf with sql
  df.registerTempTable("df1Table")
  val result1 = sqlContext.sql("select id, evenUDF(id) as even from df1Table")
  result1.show()
}
