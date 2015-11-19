package com.sachinparmar.meetup.spark.dataframe

/**
 * Created by sachinparmar on 16/11/15.
 */


/*
*  optimization examples -
*   1. constant folding
*   2. projection pruning
*   3. predicate push down
*   4. combine filters
*   5. null propagation
*   6. Boolean expression simplification
 */

object dfOptimizations extends App {
  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir)

  // ---------------------------------------------------------------------------------------

  // example 1: constant folding

  println("\n== Optimization: Constant Folding ==\n")
  val df = empDF.
    filter("1='1'").
    filter("2=2")
  utils.showLogicalPlans(df)

  // ---------------------------------------------------------------------------------------

  // example 2: projection pruning

  println("\n== Optimization: Projection Pruning ==\n")

  val df1 = empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), deptDF("dept_id"), empDF("emp_name"), empDF("salary")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name")

  /*
                  val df1 = empDF.
                            join(registerDF, registerDF("emp_id") === empDF("emp_id")).
     (A:02),(O:04,02) --->  select(empDF("emp_id"), deptDF("dept_id"), empDF("emp_name"), empDF("salary")).
                            join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
     (A:00),(O:00)    --->  select("emp_id", "salary", "dept_name")
   */

  utils.showLogicalPlans(df1)

  // ---------------------------------------------------------------------------------------

  // example 3: predicate push down

  println("\n== Optimization: Predicate Push Down ==\n")

  val df2 = df1.
    filter("salary >= 2000")

  /*
                val df2 = empDF.
                          join(registerDF, registerDF("emp_id") === empDF("emp_id")).
                          select(empDF("emp_id"), deptDF("dept_id"), empDF("emp_name"), empDF("salary")).
                          join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
                          select("emp_id", "salary", "dept_name").
   (A:00),(O:05)    --->  filter("salary >= 2000")
 */

 utils.showLogicalPlans(df2)

  // ---------------------------------------------------------------------------------------

  // example 4: combine filters

  println("\n== Optimization: Combine Filters ==\n")

  val df3 = df2.
    filter("salary < 6000")

  /*
                val df2 = empDF.
                          join(registerDF, registerDF("emp_id") === empDF("emp_id")).
                          select(registerDF("emp_id"), registerDF("dept_id"), empDF("emp_name"), empDF("salary")).
                          join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
                          select("emp_id","salary","dept_name").
   (A:00),(O:05)    --->  filter("salary >= 2000").
   (A:01),(O:05)    --->  filter("salary < 6000")
 */

  utils.showLogicalPlans(df3)

  // ---------------------------------------------------------------------------------------


  // example: null propagation
  println("\n== Optimization: Null Propagation ==\n")

  // example: Boolean expression simplification
  println("\n== Optimization: Boolean Expression Simplification ==\n")
}
