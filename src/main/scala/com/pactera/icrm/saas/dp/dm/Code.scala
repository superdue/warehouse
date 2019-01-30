package com.pactera.icrm.saas.dp.dm

import scala.collection.mutable.Stack

/*
 * 
 */
object Code extends Dimension {

  def doValue(value: Any): String = {
    value.toString()
  }

  def todo = Map(
    "value" -> doValue)

  def main(args: Array[String]) {
    println(doValue("121212"))
  }
}