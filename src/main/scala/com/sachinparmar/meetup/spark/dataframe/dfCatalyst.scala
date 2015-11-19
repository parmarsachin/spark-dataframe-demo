package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Project, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions._

/**
 * Created by sachinparmar on 17/11/15.
 */

object dfCatalyst extends App {
  init.logLevel()

  val sc = init.sparkContext
  val sqlContext = init.sqlContext(sc)

  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = true)

  df.queryExecution.analyzed

  // ---------------------------------------------------------------------------------------

  println("\n\n [#1] logical and physical plans \n\n")

  // df
  val df = empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("empname"), empDF("salary"), empDF("age")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name", "empname").
    filter("salary >= 2000").
    filter("salary < 6000")

  utils.showPlans(df, show = false)

  // ---------------------------------------------------------------------------------------

  println("\n\n [#2] logical and physical plans with cache \n\n")

  // cdf
  val cdf = empDF.
    cache().
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("empname"), empDF("salary"), empDF("age")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name", "empname").
    filter("salary >= 2000").
    filter("salary < 6000")

  //utils.showPlans(cdf, show = false)

  println("\n DF analyzed : \n\n" +  df.queryExecution.analyzed.numberedTreeString)
  println("\n DF(Cache) analyzed : \n\n" + cdf.queryExecution.analyzed.numberedTreeString)

  println("\n DF optimizedPlan : \n\n" +  df.queryExecution.optimizedPlan.numberedTreeString)
  println("\n DF(Cache) optimizedPlan : \n\n" + cdf.queryExecution.optimizedPlan.numberedTreeString)

  println("\n DF sparkPlan : \n\n" +  df.queryExecution.sparkPlan.numberedTreeString)
  println("\n DF(Cache) sparkPlan : \n\n" + cdf.queryExecution.sparkPlan.numberedTreeString)

  empDF.unpersist()

  // ---------------------------------------------------------------------------------------

  println("\n\n [#3] sql and df - optimization \n\n")

  // sql

  empDF.registerTempTable("empTable")
  deptDF.registerTempTable("deptTable")
  registerDF.registerTempTable("registerTable")

  val sql = sqlContext.sql(
      "select emp_id, salary, dept_name, empname " +
      "from deptTable join " +
      "(select empTable.emp_id, registerTable.dept_id, upper(lower(empTable.emp_name)) as empname, empTable.salary, empTable.age " +
      "from   empTable join registerTable " +
      "where  empTable.emp_id = registerTable.emp_id) x " +
      "where  x.dept_id = deptTable.dept_id "  +
      "and salary >= 2000 and salary < 6000"
  )

  println("\n DF  analyzed : \n\n" +  df.queryExecution.analyzed.numberedTreeString)
  println("\n SQL analyzed : \n\n" + sql.queryExecution.analyzed.numberedTreeString)

  println("\n DF  optimizedPlan : \n\n" +  df.queryExecution.optimizedPlan.numberedTreeString)
  println("\n SQL optimizedPlan : \n\n" + sql.queryExecution.optimizedPlan.numberedTreeString)

  // ---------------------------------------------------------------------------------------

  println("\n\n [#4] optimization provided by catalyst \n\n")

  /*
  *  optimization examples -
  *   1. constant folding √
  *   2. projection pruning √
  *   3. predicate push down √
  *   4. combine filters √
  *   ....many more
   */

  val cf = df.
    filter("1=1")

  utils.showPlans(cf, show = false)

  // ---------------------------------------------------------------------------------------

  println("\n\n [#5] extend optimizer to plug in your own rule \n\n")

  // #5: extend optimizer to plug in your own rule

  val analyzedPlan = df.queryExecution.analyzed
  println("\n analyzed (provided by Spark): \n\n" + analyzedPlan.numberedTreeString)

  val optimizedPlan = df.queryExecution.optimizedPlan
  println("\n optimizedPlan (provided by Spark) SQL: \n\n" + optimizedPlan.numberedTreeString)

  // combine filter
  val optimizedPlan1 = RuleCombineFilter(analyzedPlan)
  println("\n optimizedPlan1 (after combine filter): \n\n" + optimizedPlan1.numberedTreeString)

  // case conversion simplification
  val optimizedPlan2 = RuleCaseConversionSimplify(optimizedPlan1)
  println("\n optimizedPlan2 (after case conversion simplification): \n\n" + optimizedPlan2.numberedTreeString)

  // push filter through project
  //val optimizedPlan3 = RulePushFilterThroughProject(optimizedPlan2)
  //println("\n optimizedPlan3 (after push filter through project): \n\n" + optimizedPlan3.numberedTreeString)

  // ---------------------------------------------------------------------------------------

  // rules

  // case conversion simplification
  object RuleCaseConversionSimplify extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
      case q: LogicalPlan => q transformExpressionsUp {
        case Upper(Lower(child)) => Upper(child)
      }
    }
  }

  // combine filter
  object RuleCombineFilter extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform {
      case Filter(c1, Filter(c2, anything)) => Filter(And(c1, c2), anything)
    }
  }

  // push filter through project -- simple rule -- N/A
  object RulePushFilterThroughProject extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform  {
      case Filter(condition, Project(fields, grandChild)) =>
        Project(fields, Filter(condition, grandChild))
    }
  }
}
