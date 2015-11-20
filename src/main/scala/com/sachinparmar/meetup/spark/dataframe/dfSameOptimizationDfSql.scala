package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 16/11/15.
 */


/*
* 1. data frame and sql uses same optimization
 */

object dfSameOptimizationDfSql extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)
  import sqlContext.implicits._

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF) = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // ---------------------------------------------------------------------------------------

  println("\n\n sql and df - optimization \n\n")

  // df
  /*
  val df = empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name", "emp_name").
    filter("salary >= 2000").
    filter("salary < 5000")
  */

  val df = empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age")).
    as("x").
    join(deptDF, $"x.dept_id" === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name", "emp_name").
    filter("salary >= 2000").
    filter("salary < 5000")

  // sql

  empDF.registerTempTable("empTable")
  deptDF.registerTempTable("deptTable")
  registerDF.registerTempTable("registerTable")

  val sql = sqlContext.sql(
    "select emp_id, salary, dept_name, emp_name "
    +  "from deptTable join "
    +  "(select empTable.emp_id, registerTable.dept_id, upper(lower(empTable.emp_name)) as emp_name, empTable.salary, empTable.age "
    +  "from   empTable join registerTable "
    +  "where  empTable.emp_id = registerTable.emp_id) x "
    +  "where  x.dept_id = deptTable.dept_id "
    +  "and    salary >= 2000 and salary < 6000"
  )

  println("\n DF  analyzed : \n\n" +  df.queryExecution.analyzed.numberedTreeString)
  println("\n SQL analyzed : \n\n" + sql.queryExecution.analyzed.numberedTreeString)

  println("\n DF  optimizedPlan : \n\n" +  df.queryExecution.optimizedPlan.numberedTreeString)
  println("\n SQL optimizedPlan : \n\n" + sql.queryExecution.optimizedPlan.numberedTreeString)
}
/*
DF
06 05 04
03 (06 05 04) 07
02 (03 (06 05 04) 07)
01 (02 (03 (06 05 04) 07)) 08
00 (01 (02 (03 (06 05 04) 07)) 08)

SQL
07 06 05
04 (07 06 05) 08
03 (04 (07 06 05) 08)
01 (03 (04 (07 06 05) 08)) 02
00 (01 (03 (04 (07 06 05) 08)) 02)
*/