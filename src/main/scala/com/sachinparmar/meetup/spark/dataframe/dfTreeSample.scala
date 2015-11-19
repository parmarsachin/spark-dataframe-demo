package com.sachinparmar.meetup.spark.dataframe

/**
 * Created by sachinparmar on 16/11/15.
 */

/*
* simple tree and transform example
*
 */

import org.apache.spark.sql.catalyst.expressions._

object dfTreeSample extends App {

  /*
  Simple Tree representation

             *
            / \
           /   \
          +     3
         / \
        /   \
       1     2
  */

  // scala representation of the tree (tree nodes)
  // Multiply, Add, Literal are subclasses of TreeNode

val op = Multiply(Add(Literal(1),Literal(2)), Literal(3))

println(op)

  // rule
  // someFunction(tree#1) --> tree#2
  // set of pattern matching functions that find and replace subtrees with a specific structure
  // transform method applies a pattern matching function recursively on all nodes of the tree
  // partial function -- it only needs to match to a subset of all possible input trees, skips other part of tree
  // rules do not need to be modified as new types of operators are added to the system

val op_with_my_rule = op transform  {
    case Add(a,b) => Subtract(a,b)
  }

  println(op_with_my_rule)

  /*
             *
            / \
           /   \
          -     3
         / \
        /   \
       1     2
  */

  // multiple transformations/rules at once

  val op_with_my_rule2 = op transform  {
    case Add(a,b) => Subtract(a,b)
    case Multiply(a,b) => Add(a,b)
  }

  println(op_with_my_rule2)

  /*
             +
            / \
           /   \
          -     3
         / \
        /   \
       1     2
  */
}
