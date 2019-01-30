package com.pactera.icrm.saas.dp.dm

import scala.collection.mutable.Stack

object Date extends Dimension {
  def getDateMark(filter: Map[String, (String, String, String)]) = {
    Date.todo.toList.sortBy(_._1).map(x => {
      filter.get(x._1) match {
        case Some(y) => x._1 + y._2 + y._3
        case None    => "x"
      }
    }).mkString("-")
  }
  def getDateMark2(filter: Map[String, (String, String)]) = {
    Date.todo.toList.sortBy(_._1).map(x => {
      filter.get(x._1) match {
        case Some(y) => x._1 + y._1 + y._2
        case None    => "x"
      }
    }).mkString("-")
  }
  def doYear: Any => String = value => {
    value match {
      case value: String =>
        value.substring(0, 4)
      case _ => ""
    }
  }
  def doHalfYear(value: Any): String = {
    value match {
      case value: String =>
        if (value.substring(4, 6).toInt > 6) "2" else "1"
      case _ => ""
    }
  }
  def doQuater(value: Any): String = {
    value match {
      case value: String =>
        (value.substring(4, 6).toLong - 1) / 3 + 1 + ""
      case _ => ""
    }
  }
  def doMonth(value: Any): String = {
    value match {
      case value: String =>
        value.substring(4, 6).toInt + ""
      case _ => ""
    }
  }
  def doDay(value: Any): String = {
    value match {
      case value: String =>
        value.substring(6, 8).toInt + ""
      case _ => ""
    }
  }

  def todo = Map(
    "year" -> doYear,
    "halfYear" -> doHalfYear,
    "quarter" -> doQuater,
    "month" -> doMonth,
    "day" -> doDay)

  def main(args: Array[String]) {
    val dateVal: List[String] = List("20171013", "20181214", "20161013", "20121013", "20161015", "20161213")
    val dateCfg = (100, List(
      (1, "year", 9999L),
      (2, "hyear", 9L),
      (3, "quarter", 9L),
      (4, "month", 99L),
      (5, "date", 99L)))

    val dateScope = Map(
      "year" -> Map("in" -> List(2016, 2017)),
      "month" -> Map("notin" -> List(12, 11)),
      "date" -> Map("notin" -> List(12, 13)))
    val dateFilterExp = "$year&$month&$date"
    println(dateScope)
    val dateFilter = Map(
      "booleans" -> dateScope,
      "calcExp" -> dateFilterExp)
  }
}