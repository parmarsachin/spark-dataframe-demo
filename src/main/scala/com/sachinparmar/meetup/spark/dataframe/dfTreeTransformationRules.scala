package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{Project, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 16/11/15.
 */

/*
* apply transform rules on analysed plan to get the optimized plan
 */



object dfTreeTransformationRules extends App {

  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // ---------------------------------------------------------------------------------------

  val df = empDF.
    select(empDF("emp_id"),upper(lower(empDF("emp_name"))), empDF("salary")).
    filter("salary > 2000").
    filter("salary < 5000")


  val logicalPlan = df.queryExecution.logical
  println("\n logicalPlan (provided by Spark): \n" + logicalPlan.numberedTreeString)

  // analysed

  val analyzedPlan = df.queryExecution.analyzed
  println("\n analyzed (provided by Spark): \n" + analyzedPlan.numberedTreeString)

// my rules

  // combine filter
  val optimizedPlan1 = RuleCombineFilter(analyzedPlan)
  println("\n optimizedPlan1 (after combine filter): \n" + optimizedPlan1.numberedTreeString)

  // case conversion simplification
  val optimizedPlan2 = RuleCaseConversionSimplify(optimizedPlan1)
  println("\n optimizedPlan2 (after case conversion simplification): \n" + optimizedPlan2.numberedTreeString)

  // push filter through project
  val optimizedPlan3 = RulePushFilterThroughProject(optimizedPlan2)
  println("\n optimizedPlan3 (after push filter through project): \n" + optimizedPlan3.numberedTreeString)

  // push filter through project
  val optimizedPlan4 = newRule(optimizedPlan3)
  println("\n optimizedPlan4 (after push filter through project): \n" + optimizedPlan4.numberedTreeString)

 //

  //var optimizer: Optimizer = utils.getOptimizer(sqlContext)
  //optimizer.execute(optimizedPlan4)


  df.show()
  df.printSchema()

  val optimizedPlan = df.queryExecution.optimizedPlan
  println("\n after optimizedPlan (provided by Spark): \n" + optimizedPlan.numberedTreeString)


  // ---------------------------------------------------------------------------------------





  // combine filter
  object RuleCombineFilter extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform {
      case Filter(c1, Filter(c2, anything)) => Filter(Or(c1, c2), anything)
    }
  }

  // case conversion simplification
  object RuleCaseConversionSimplify extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
      case q: LogicalPlan => q transformExpressionsUp {
        case Upper(Lower(child)) => Lower(child)
      }
    }
  }

  // push filter through project -- simple rule
  object RulePushFilterThroughProject extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
      case Filter(condition, Project(fields, grandChild)) =>
        Project(fields, Filter(condition, grandChild))
    }
  }

  object newRule extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
      case Filter(condition, grandChild) => grandChild
    }
  }
}