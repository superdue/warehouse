package com.pactera.icrm.saas.dp.base

import scala.collection.mutable.Stack

/*
 * tbCfg:Map[表名称，Map[字段名,关联表名称]]
 * keyMap:Map[表名称，主键名]
 */
class DmJoin(tbCfg: Map[String, Map[String, String]], keyMap: Map[String, String]) {

}
object DmJoin {
  def getDmJoinTable(fact: String,
                     tbCfg: Map[String, Map[String, String]]) = {
    tbCfg.get(fact).map(_.values.toSet.filter(x => {x != ""}))
  }
  def getDmJoinSql(fact: String,
                   tbCfg: Map[String, Map[String, String]],
                   keyMap: Map[String, String]) = {
    val stack = new Stack[Char]
    stack.push('a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'o', 'p', 'r', 's', 't', 'u')
    var columns: String = "select "
    var from: String = " from "
    tbCfg.get(fact).map(table => {
      val mainTbAs = stack.pop()
      from = from + fact + " " + mainTbAs
      table.map(field => {
        tbCfg.get(field._2) match {
          case None => columns = columns + mainTbAs + "." + field._1 + ","
          case Some(joinTable) => {
            val prefix = stack.pop()
            val key = keyMap.get(field._2).getOrElse("none")
            columns = columns + joinTable.-("dttm").keys.map(prefix + "." + _).mkString(",") + ","
            from = from + " left join " + field._2 + " " + prefix + " on " + mainTbAs + "." + field._1 +
              " = " + prefix + "." + key
          }
        }
      })
    })
    val sql = columns.dropRight(1) + from
    if (sql.size > 14) Some(sql)
    else None
  }
  def main(args: Array[String]) {
    val tbcfg = Map("tbA" -> Map("field1" -> "", "field2" -> "", "field3" -> "tbB", "field4" -> "tbC"),
      "tbB" -> Map("fd1" -> "", "fd2" -> ""),
      "tbC" -> Map("df1" -> "", "df2" -> ""))
    val keycfg = Map("tbA" -> "field1", "tbB" -> "fd1")
    val sql = DmJoin.getDmJoinSql("tbA", tbcfg, keycfg)
    println(sql)
  }
}