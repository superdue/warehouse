package com.pactera.icrm.saas.test

object varParam extends App {
  def echo(args: String*) =
    for (arg <- args) println(arg)
  echo("asdf", "121", "12")
  val tmp = List("123", "123", "111")
  echo(tmp: _*)
}