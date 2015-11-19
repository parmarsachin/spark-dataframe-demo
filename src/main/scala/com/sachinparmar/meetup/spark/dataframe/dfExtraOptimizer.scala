package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.catalyst.expressions.{Lower, Upper}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 18/11/15.
 */

// custom optimizer with custom rules for tree transformation

object MyOptimizer extends Optimizer {
  val batches =
      Batch("Remove Filters", FixedPoint(100), RuleRemoveFilter) ::
      Batch("Consider Upper as Lower", FixedPoint(100), RuleCaseConversionSimplify) ::
      Nil
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

// main

object dfExtraOptimizer extends App {
  init.logLevel()

  val sc = init.sparkContext

  val sqlContext = init.sqlContext(sc, default = false)

  val dataDir = init.resourcePath
  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  val df = empDF.
    select(empDF("emp_id"),upper(lower(empDF("emp_name"))), empDF("salary")).
    filter("salary > 2000").
    filter("salary < 5000")

  df.show()
  df.printSchema()

  val logicalPlan = df.queryExecution.logical
  println("\n logicalPlan (provided by Spark): \n" + logicalPlan.numberedTreeString)

  val optimizedPlan = df.queryExecution.optimizedPlan
  println("\n after optimizedPlan (provided by Spark): \n" + optimizedPlan.numberedTreeString)
}
