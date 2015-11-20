package com.sachinparmar.meetup.spark.dataframe

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.optimizer.{DefaultOptimizer, Optimizer}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by sachinparmar on 16/11/15.
 */

// custom optimizer with custom rules for tree transformation

class MyOptimizer(rules: Map[String, Rule[LogicalPlan]], includeDefaultOptimizer: Boolean = false) extends Optimizer {

  val my_batch = rules.map {
    case (ruleName,rule) => Batch(ruleName, FixedPoint(100), rule)
  }

  var default_batches: Seq[Batch] = Seq()

  if(includeDefaultOptimizer)
  {
      default_batches = DefaultOptimizer.batches.map(
        batch => new Batch(batch.name, FixedPoint(100),batch.rules:_ *)
      ).toSeq
  }

  val batches = default_batches ++ my_batch.toSeq ++ Nil
}

// custom sql context with custom optimizer

class CustomSQLContext(sc: SparkContext, co: Optimizer) extends SQLContext(sc) {
  override lazy val optimizer: Optimizer = co
}

object init {

  // log level
  def logLevel() = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  // spark context
  def sparkContext: SparkContext = {
    val sparkConf = new SparkConf().setAppName("demo").setMaster("local[*]")
    new SparkContext(sparkConf)
  }

  // sql context / custom sql context
  def sqlContext(sc: SparkContext, co: Optimizer = null) = {
    if (co == null) {
      new SQLContext(sc)
    }
    else {
      new CustomSQLContext(sc, co)
    }
  }

  // sample data path
  def resourcePath: String = {
    "/Users/sachinparmar/my/work/myGit/spark-dataframe-demo/src/main/resources/"
  }

  // sample data frames for demo
  def sampleDataFrameForJoin(sqlContext: SQLContext, dataDir: String, show: Boolean = true): (DataFrame, DataFrame, DataFrame) = {
    val empDFSchema =
      StructType(
        StructField("emp_id", IntegerType, nullable = false) ::
          StructField("emp_name", StringType, nullable = true) ::
          StructField("salary", IntegerType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) :: Nil)
    val empDF = sqlContext.read.schema(empDFSchema).json(dataDir + "emp.json")

    val deptDFSchema =
      StructType(
        StructField("dept_id", IntegerType, nullable = false) ::
          StructField("dept_name", StringType, nullable = true) :: Nil)
    val deptDF = sqlContext.read.schema(deptDFSchema).json(dataDir + "dept.json")

    val registerDFSchema =
      StructType(
        StructField("emp_id", IntegerType, nullable = false) ::
          StructField("dept_id", IntegerType, nullable = true) :: Nil)
    val registerDF = sqlContext.read.schema(registerDFSchema).json(dataDir + "register.json")

    if(show) {
      empDF.show()
      empDF.printSchema()

      deptDF.show()
      deptDF.printSchema()

      registerDF.show()
      registerDF.printSchema()
    }

    (empDF, deptDF, registerDF)
  }



/*
  def sampleDataFrame(sqlContext: SQLContext, dataDir: String): DataFrame = {
    case class SampleSchema(id: Int, name: String, city: String)

    val rdd = sqlContext.sparkContext.
      textFile(dataDir + "sample-data.csv").
      map(_.split(",")).
      map(r => SampleSchema(r(0).toInt, r(1), r(2)))

    import sqlContext.implicits._

    val df = rdd.toDF("id", "name", "city")

    df
  }
*/
}

object utils {
  // print logical plans with data frame
  def showLogicalPlans(df: DataFrame, show: Boolean = true) = {
    if(show) {
      df.show()
    }

    println("\n== Parsed Logical Plan ==\n" + df.queryExecution.logical.numberedTreeString)
    println("\n== Analyzed Logical Plan ==\n" + df.queryExecution.analyzed.numberedTreeString)
    println("\n== With Cached Data Logical Plan ==\n" + df.queryExecution.withCachedData.numberedTreeString) // same as analyzed if no cache
    println("\n== Optimized Logical Plan ==\n" + df.queryExecution.optimizedPlan.numberedTreeString)
  }

  // prints physical plans
  def showPhysicalPlans(df: DataFrame) = {
    println("\n== Spark Plan ==\n" + df.queryExecution.sparkPlan.numberedTreeString)
    println("\n== Physical/Executed Plan ==\n" + df.queryExecution.executedPlan.numberedTreeString)
  }

  // prints logical and physical plans
  def showPlans(df: DataFrame, show: Boolean = true) = {
    showLogicalPlans(df, show)
    showPhysicalPlans(df)
  }
}

/*
  init.logLevel

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)
  import sqlContext.implicits._

  val dataDir = init.resourcePath

  val df = init.sampleDataFrame(sc, sqlContext, dataDir)
  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir)
 */


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