package com.pactera.icrm.saas.test

import java.util.Properties
import java.io.FileInputStream

class Prop {
  private val prop: Properties = loadProperties("config/jdbc.prop")
  private val jdbc: Map[String, String] = Map(
    "url" -> prop.getProperty("url"),
    "driver" -> prop.getProperty("driver"),
    "user" -> prop.getProperty("user"),
    "password" -> prop.getProperty("password"),
    "characterEncoding" -> prop.getProperty("characterEncoding"))

  /*加载属性文件
   * */
  def loadProperties(fileName: String): Properties = {
    val prop = new Properties()
    val path: String = Thread.currentThread().getContextClassLoader.getResource(fileName).getPath
    prop.load(new FileInputStream(path))
    prop
  } 
}