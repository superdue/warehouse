package com.pactera.icrm.saas.dp.base

import scala.collection.mutable.Stack

object Comm {
    def dateFilter(value: Map[String, Any],
                 filterExp: String,
                 filter: Map[String, (String, String, String)]) = {
    println("values:" + value)
    println("filter:" + filter)
    val res = filter.map(f => {
      val field = f._1
      val boolean = f._2._2 match {
        case "="     => value.get(field).getOrElse("-1") == f._2._3
        case "!="    => value.get(field).getOrElse("-1") != f._2._3
        case "in"    => f._2._3.split(",").contains(value.get(field).getOrElse("-1"))
        case "notin" => !f._2._3.split(",").contains(value.get(field).getOrElse("-1"))
        case _       => false
      }
      (field, boolean)
    })
    println("res:" + res)
    val operSeq = infix2postfix(filterExp).split(',').toList
    sumBoolean(operSeq, res)
  }

  def infix2postfix(exp: String): String = {
    val stack = new Stack[String]
    var ret: String = ""
    //    for (c <- tst) {
    var field = ""
    for (c <- exp) {
      if (c == '&' || c == '|' || c == '(' || c == ')') {
        if (field.size > 0) {
          ret = ret + field + ","
          field = ""
        }
        if (c == '&' || c == '|' || c == '(') {
          stack.push(c + ",")
        } else if (c == ')') {
          var tmp: String = ""
          while (stack.size > 0 && tmp != "(,") {
            tmp = stack.pop()
            if (tmp != "(,")
              ret = ret + tmp
          }
        } else if (c == ' ') {}
        else {}
      } else {
        field += c
      }
    }
    if (field.size > 0) ret = ret + field + ","
    while (stack.size > 0) {
      ret = ret + stack.pop()
    }
    //    println(ret)
    ret
  }
  /*
   * input:
   * 	(List[操作符及操作数key],Map(操作数key,操作数值)
   * return：
   * 	计算结果
   */
  def sumBoolean(sumExp: List[String], booleans: Map[String, Boolean]): Boolean = {
    val stack = Stack[Boolean]()
    for (p <- sumExp) {
      p match {
        case "|" => stack.push(stack.pop() || stack.pop())
        case "&" => stack.push(stack.pop() && stack.pop())
        case _ => {
          //          println(booleans.get(p))
          stack.push(booleans.get(p).getOrElse(false))
        }
      }
    }
    stack.pop()
  }
}