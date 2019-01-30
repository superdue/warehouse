package com.pactera.icrm.saas.dp.dm

import scala.collection.mutable.Stack
import java.text.SimpleDateFormat

object Birth extends Dimension {

  def getAge(birth: String) = {
    val age = TaskPub.workDate.substring(0, 4).toInt - birth.substring(0, 4).toInt
    val month = if (TaskPub.workDate.substring(4, 8) > birth.substring(4, 8)) 0 else -1
    age + month
  }

  def doYears(value: Any):String = {
    value match {
      case value: String =>
        value.substring(3, 4)
      case _           => ""
    }
  }
  def doAges(value: Any):String = {
    value match {
      case value: String => getAge(value).toString
      case _           => ""
    }
  }

  def todo = Map(
    "years" -> doYears,
    "ages" -> doAges
    )

  def main(args: Array[String]) {
    println(doAges("19870909"))
  }
}