package com.pactera.icrm.saas.dp.dm

trait Dimension {
  def todo: Map[String, Any => Any]
}