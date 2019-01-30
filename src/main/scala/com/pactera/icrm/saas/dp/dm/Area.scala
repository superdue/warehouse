package com.pactera.icrm.saas.dp.dm

import scala.collection.mutable.Stack

/*
 * 
 */
object Area extends Dimension {

  def doProvence(value: Any) = {
    value match {
      case value: String => {
        value.substring(0, 2)
      }
      case _ => {
        0L
      }
    }
  }

  def doCity(value: Any) = {
    value match {
      case value: String =>
        value.substring(2, 4)
      case _ => 0L
    }
  }

  def doCounty(value: Any) = {
    value match {
      case value: String =>
        value.substring(4, 6)
      case _ => 0L
    }
  }
  def todo = Map(
    "provence" -> doProvence,
    "city" -> doCity,
    "county" -> doCounty)

  def main(args: Array[String]) {
    println(doProvence("121212"))
  }
}