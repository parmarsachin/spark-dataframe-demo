package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.catalyst.expressions.{Lower, Upper}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 18/11/15.
 */


/*
* 1. sqlContext with default optimizer
* 2. custom sqlContext with custom optimizer
* // includeDefaultOptimizer = false
* 3. custom sqlContext with default + custom optimizer
* // includeDefaultOptimizer = true
 */

object dfExtraOptimizer extends App {

  init.logLevel()

  val sc = init.sparkContext

  // run#1

  //val sqlContext = init.sqlContext(sc)

  // run#2 - includeDefaultOptimizer = false
  // run#3 - includeDefaultOptimizer = true

  // custom optimizer
  // my optimizer
  val co = new CustomOptimizer(
    Map(
      "RuleRemoveFilter"            -> RuleRemoveFilter,
      "RuleCaseConversionSimplify"  -> RuleCaseConversionSimplify
    ),
    includeDefaultOptimizer = false
  )
  val sqlContext = init.sqlContext(sc, co = co)

  import sqlContext.implicits._
  val dataDir = init.resourcePath
  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // df
  val df = empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name", "emp_name").
    filter("salary >= 2000").
    filter("salary < 5000")

  df.show()
  df.printSchema()

  // ---------------------------------------------------------------------------------------

  val analyzed = df.queryExecution.analyzed
  println("\n Analyzed Plan : \n" + analyzed.numberedTreeString)

  val optimizedPlan = df.queryExecution.optimizedPlan
  println("\n Optimized Plan : \n" + optimizedPlan.numberedTreeString)
}


// custom rules for tree transformation

// rule: remove filters
object RuleRemoveFilter extends Rule[LogicalPlan]
{
  override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
    case Filter(condition, grandChild) => grandChild
  }
}

// rule: case conversion simplification
object RuleCaseConversionSimplify extends Rule[LogicalPlan]
{
  override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
    case q: LogicalPlan => q transformExpressionsUp {
      case Upper(Lower(child)) => Lower(child)
    }
  }
}
