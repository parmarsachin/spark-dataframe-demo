package com.sachinparmar.meetup.spark.dataframe

/**
 * Created by sachinparmar on 16/11/15.
 */


/*
* optimized plan with data frame and sql
* // change the sql and put another example
 */

object dfJoinExplain extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir)

  // ---------------------------------------------------------------------------

  println("\n\n...DF...\n\n")

  val resultDF =
    empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select(empDF("emp_id"), deptDF("dept_id"), empDF("emp_name"), empDF("salary"))

  resultDF.show()
  //utils.showLogicalPlans(resultDF)
  println("\n\n optimizedPlan (DF) \n\n" + resultDF.queryExecution.optimizedPlan.numberedTreeString)

  // ---------------------------------------------------------------------------

  println("\n\n...SQL...\n\n")

  empDF.registerTempTable("empTable")
  deptDF.registerTempTable("deptTable")
  registerDF.registerTempTable("registerTable")

  val resultSQL = sqlContext.sql(
      "select empTable.emp_id, deptTable.dept_id, empTable.emp_name, empTable.salary " +
      "from   empTable join registerTable join deptTable " +
      "where  empTable.emp_id = registerTable.emp_id " +
      "and    registerTable.dept_id = deptTable.dept_id")

  resultSQL.show()
  //utils.showLogicalPlans(resultSQL)
  println("\n\n optimizedPlan (SQL) \n\n" + resultSQL.queryExecution.optimizedPlan.numberedTreeString)
}
