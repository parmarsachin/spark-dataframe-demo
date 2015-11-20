package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 16/11/15.
 */


/*
*  default optimizer in catalyst provides many optimization rules like -
*   1. constant folding
*   2. projection pruning
*   3. predicate push down
*   4. combine filters
*   many more .......
 */

object dfOptimizations extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // ---------------------------------------------------------------------------------------

  // df
  val df = empDF
    .join(registerDF, registerDF("emp_id") === empDF("emp_id"))
    //.select(empDF("emp_id"), registerDF("dept_id"), empDF("emp_name"), empDF("salary"), empDF("age"))
    .select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age"))
    .join(deptDF, registerDF("dept_id") === deptDF("dept_id"))
    .select("emp_id", "salary", "dept_name", "emp_name")
    .filter("salary >= 2000")
    .filter("salary < 5000")

  // ---------------------------------------------------------------------------------------

  println("\n\n [#4] optimization provided by catalyst \n\n")

  val cf = df.
    filter("1=1")

  println("\n DF  analyzed : \n\n" +  cf.queryExecution.analyzed.numberedTreeString)

  println("\n DF  optimizedPlan : \n\n" +  cf.queryExecution.optimizedPlan.numberedTreeString)

}
