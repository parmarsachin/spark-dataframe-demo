package com.sachinparmar.meetup.spark.dataframe

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
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

  import sqlContext.implicits._
  val dataDir = init.resourcePath

  val (empDF, deptDF, registerDF)  = init.sampleDataFrameForJoin(sqlContext, dataDir, show = false)

  // ---------------------------------------------------------------------------------------

  // df
  val df = empDF.
    join(registerDF, registerDF("emp_id") === empDF("emp_id")).
    select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age")).
    join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
    select("emp_id", "salary", "dept_name", "emp_name").
    filter("salary >= 2000").
    filter("salary < 5000")

  // ---------------------------------------------------------------------------------------

  // analysed

  val analyzedPlan = df.queryExecution.analyzed
  println("\n analyzed (provided by Spark): \n" + analyzedPlan.numberedTreeString)

  val optimizedPlan = df.queryExecution.optimizedPlan
  println("\n optimizedPlan (provided by Spark): \n" + optimizedPlan.numberedTreeString)

  // custom rules

  // combine filter
  val optimizedPlan1 = RuleCombineFilter(analyzedPlan)
  println("\n optimizedPlan1 (after combine filter): \n" + optimizedPlan1.numberedTreeString)

  // case conversion simplification
  val optimizedPlan2 = RuleCaseConversionSimplify(optimizedPlan1)
  println("\n optimizedPlan2 (after case conversion simplification): \n" + optimizedPlan2.numberedTreeString)

  // ---------------------------------------------------------------------------------------

  // scala representation of the tree (tree nodes)
  // Multiply, Add, Literal are subclasses of TreeNode
  // rule
  // someFunction(tree#1) --> tree#2
  // set of pattern matching functions that find and replace subtrees with a specific structure
  // transform method applies a pattern matching function recursively on all nodes of the tree
  // partial function -- it only needs to match to a subset of all possible input trees, skips other part of tree
  // rules do not need to be modified as new types of operators are added to the system

/*
Tree representation of ..... Filter(c1, Filter(c2, restOfTheSubTree))

           Filter
          /      \
         /        \
        Filter    c1
       / \
      /   \
     c2   restOfTheSubTree

Tree representation of ..... Filter(And(c1, c2), restOfTheSubTree)

           Filter
          /      \
         /        \
        And    restOfTheSubTree
       / \
      /   \
     c1    c2

*/

  // combine filter
  object RuleCombineFilter extends Rule[LogicalPlan]
  {
    override def apply(lp: LogicalPlan): LogicalPlan = lp transform {
      case Filter(c1, Filter(c2, restOfTheSubTree)) => Filter(And(c1, c2), restOfTheSubTree)
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
}