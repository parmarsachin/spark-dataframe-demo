package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}

/**
 * Created by sachinparmar on 16/11/15.
 */


object dfCreate2 extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  // schema
  val schema =
    StructType(
      StructField("id", IntegerType, nullable = false) ::
        StructField("name", StringType, nullable = true) ::
        StructField("city", StringType, nullable = true) :: Nil)

  /*
  val d1 = Array(1,2,3,4).map(v => (v+1))
  val d2 = Array(1,2,3,4).map(v => (v+2))
  val r1 = Row.fromSeq(d1)
  val r2 = Row.fromSeq(d2)
  val rdd = sqlContext.sparkContext.makeRDD(IndexedSeq(r1,r2))
  */

  // rows of data
  val data = (0 to 15).map(v => {
    val r = Array(v, "name#"+(v + 5).toString, "city#"+(v + 10).toString)
    Row.fromSeq(r)
  })

  // rdd
  val rdd = sqlContext.sparkContext.makeRDD(data)

  // data frame
  val df = sqlContext.createDataFrame(rdd, schema)

  println("Schema: ")
  df.printSchema()

  println("Data: ")
  df.show()

  //utils.showPlans(df)

  // sample data frame creation
  import sqlContext.implicits._

  val df1 = sc.parallelize(Seq(1,2,3,4)).toDF("id")
  df1.show()

  val df2 = sqlContext.range(0, 100)
  df2.show()
}


