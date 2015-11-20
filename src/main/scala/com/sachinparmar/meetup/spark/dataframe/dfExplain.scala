package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 16/11/15.
 */

/*
*   1. logical and physical plans
*   2. logical and physical plan with cache
*/

object dfExplain extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // ---------------------------------------------------------------------------------------

  println("\n\n [#1] logical and physical plans \n\n")

  // df
  val df = empDF
    .join(registerDF, registerDF("emp_id") === empDF("emp_id"))
    //.select(empDF("emp_id"), registerDF("dept_id"), empDF("emp_name"), empDF("salary"), empDF("age"))
    .select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age"))
    .join(deptDF, registerDF("dept_id") === deptDF("dept_id"))
    .select("emp_id", "salary", "dept_name", "emp_name")
    .filter("salary >= 2000")
    .filter("salary < 5000")

  utils.showPlans(df, show = false)

  /*
  df.queryExecution.logical.numberedTreeString
  df.queryExecution.analyzed.numberedTreeString
  df.queryExecution.withCachedData.numberedTreeString
  df.queryExecution.optimizedPlan.numberedTreeString

  df.queryExecution.sparkPlan.numberedTreeString
  df.queryExecution.executedPlan.numberedTreeString
  */

  // ---------------------------------------------------------------------------------------

  println("\n\n [#2] logical and physical plans with cache \n\n")

  // cdf
  val cdf = empDF
    .cache
    .join(registerDF, registerDF("emp_id") === empDF("emp_id"))
    //.select(empDF("emp_id"), registerDF("dept_id"), empDF("emp_name"), empDF("salary"), empDF("age"))
    .select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age"))
    .join(deptDF, registerDF("dept_id") === deptDF("dept_id"))
    .select("emp_id", "salary", "dept_name", "emp_name")
    .filter("salary >= 2000")
    .filter("salary < 5000")

  //utils.showPlans(cdf, show = false)

  println("\n DF analyzed : \n\n" +  df.queryExecution.analyzed.numberedTreeString)
  println("\n DF(Cache) analyzed : \n\n" + cdf.queryExecution.analyzed.numberedTreeString)

  println("\n DF optimizedPlan : \n\n" +  df.queryExecution.optimizedPlan.numberedTreeString)
  println("\n DF(Cache) optimizedPlan : \n\n" + cdf.queryExecution.optimizedPlan.numberedTreeString)

  println("\n DF sparkPlan : \n\n" +  df.queryExecution.sparkPlan.numberedTreeString)
  println("\n DF(Cache) sparkPlan : \n\n" + cdf.queryExecution.sparkPlan.numberedTreeString)

  //empDF.unpersist()
}
