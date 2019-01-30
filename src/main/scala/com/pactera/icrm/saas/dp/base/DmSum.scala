package com.pactera.icrm.saas.dp.base

import scala.collection.mutable.Stack

object DmSum {
  def main(args: Array[String]) {
    val table = "tabla"
    val selectCfg = Map("filed1" -> "", "field2" -> "sum", "field3" -> "", "f4" -> "avg")
    val whereCfg = Map("f5" -> ("f5", "in", "(1, 2)"), "f6" -> ("f6", "=", "1"))
    val boolExp = "((f5&f6)|f7)&f8"
    val cfg = (table, selectCfg, whereCfg, boolExp)
    println(createSumSql(table, boolExp, selectCfg, whereCfg))
  }

  def createSumSql(table: String,
                   filterExp: String,
                   fields: Map[String, String],
                   filter: (Map[String, (String, String, String)])) = {

    val columns = fields.map(f => {
      if (f._2 == "")
        f._1
      else
        f._2 + "(" + f._1 + ")" + " as " + f._1
    }).mkString(",")
    val groupBy = fields.filter(x => {
      x._2 == ""
    })

    val groupStr = if (groupBy.size == fields.size)
      ""
    else
      " group by " + groupBy.keys.mkString(",")

    val whereMap = filter.map(w => {
      (w._1, w._2._1 + " " + w._2._2 + " " + w._2._3)
    })

    var field = ""
    var whereStr = " where "

    val transfer: Char => Any = v => {
      if (v == '&') " and "
      else if (v == '|') " or "
      else v
    }
    if (whereMap.size > 0 && filterExp.size > 0) {
      for (c <- filterExp) {
        if (c == '&' || c == '|' || c == '(' || c == ')') {
          if (field.size > 0) {
            val exp = whereMap.get(field).getOrElse("(1=2)")
            field = ""
            whereStr += exp + transfer(c)
          } else
            whereStr += transfer(c)
        } else {
          field += c
        }
      }
    } else {
      whereStr = ""
    }
    val sql = "select " + columns + " from " + table + whereStr + whereMap.get(field).getOrElse("") + groupStr
    if (fields.size > 0) Some(sql)
    else None
  }
}