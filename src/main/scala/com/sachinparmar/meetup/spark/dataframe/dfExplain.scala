package com.sachinparmar.meetup.spark.dataframe

/**
 * Created by sachinparmar on 16/11/15.
 */

/*
* show logical and physical plans with/without cache
 */
object dfExplain extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath
  val (empDF, deptDF, registerDF) = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // ---------------------------------------------------------------------------

  val df = empDF.
    //cache().
    filter(empDF("emp_name") !== "sachin").
    filter(empDF("emp_id") !== 4)

  df.show()

  /*
  df.queryExecution.logical.numberedTreeString
  df.queryExecution.analyzed.numberedTreeString
  df.queryExecution.withCachedData.numberedTreeString
  df.queryExecution.optimizedPlan.numberedTreeString

  df.queryExecution.sparkPlan.numberedTreeString
  df.queryExecution.executedPlan.numberedTreeString
  */

  utils.showPlans(df)

  // with cache

  val df1 = empDF.
    cache().
    filter(empDF("emp_name") !== "sachin").
    filter(empDF("emp_id") !== 4)

  utils.showPlans(df1)
}

/*
import org.apache.spark.sql.types._

val dataDir = "/Users/sachinparmar/my/work/myGit/spark-dataframe-demo/src/main/resources/"

val empDFSchema =
StructType(
StructField("emp_id", IntegerType, false) ::
StructField("emp_name", StringType, true) ::
StructField("salary", IntegerType, true) :: Nil)
val empDF = sqlContext.read.schema(empDFSchema).json(dataDir + "emp.json")

val deptDFSchema =
StructType(
StructField("dept_id", IntegerType, false) ::
StructField("dept_name", StringType, true) :: Nil)
val deptDF = sqlContext.read.schema(deptDFSchema).json(dataDir + "dept.json")

val registerDFSchema =
StructType(
StructField("emp_id", IntegerType, false) ::
StructField("dept_id", IntegerType, true) :: Nil)
val registerDF = sqlContext.read.schema(registerDFSchema).json(dataDir + "register.json")

empDF.show
deptDF.show
registerDF.show

*/
