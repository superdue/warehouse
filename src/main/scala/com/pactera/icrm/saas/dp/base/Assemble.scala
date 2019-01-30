package com.pactera.icrm.saas.dp.base

import scala.collection.mutable.Stack
import javafx.scene.control.TableCell
import com.pactera.icrm.saas.dp.dbtable.TableCfg

object Assemble {
  /*
    * input:
    * 	(AssembleId,List[(aliasName,expression)])
    * 	Map(index_name -> Map(fieldName,api))
    */
  def getAssembleSql(assemble: (String, List[(String, String)]),
                     indexFlds: Map[String, Map[String, String]]) = {
    val indexes = assemble._2.map(oneInd => {
      getIndexFromExp(oneInd._2)
    })
    val indexSet = indexes.foldLeft(Set[String]())((r, e) => {
      r.++(e)
    }).toList

    /*
     * filter field with api
     */
    val dms = indexSet.map(i => {
      (i, indexFlds.get(i).map(_.filter(x => x._2 == "")))
    })
    val dmHead = dms.headOption.map(_._2.map(_.keys.toList)).getOrElse(None)
    val joinDm = dmHead.map(h =>
      dms.foldLeft(h)((r, e) => {
        val el = e._2.map(_.keys.toList)
        if (r.equals(el.get)) r
        else Nil
      }))

    val selectStr = assemble._2.map(i => {
      (i._2) + " as " + i._1
    }).mkString(",")

    if (indexSet.size > 0) {
      val mainInd = indexSet.head
      val joinStr = indexSet.drop(1).map(i => {
        val onStr = joinDm.map(_.map(d => {
          mainInd + "." + d + " = " + i + "." + d
        }))
        if (onStr != None)
          " inner join " + i + " on " + onStr.get.mkString(" and ")
        else ""
      }).mkString(" ")

      if (selectStr.size > 1 && mainInd.size > 1) {
        val sqlStr = "select " + selectStr + " from " + mainInd + joinStr
        Some(indexSet,sqlStr)
      } else None
    } else None
  }

  def getIndexFromExp(exp: String) = {
    val tables = new Stack[String]
    var table = ""
    for (c <- exp) {
      if (c == '+' || c == '-' || c == '*' || c == '/' || c == '(' || c == ')' || c == ' ') {
        table = ""
      } else {
        if (c == '.') tables.push(table)
        else table += c
      }
    }
    tables.toList
  }
  /*
   * 根据指标名称，获取指标当前的时间维
   */
  def getDateDmBy(indexNames:List[String]) = {
    
  }

  def main(args: Array[String]) {
    val select1 = Map("f1" -> "", "f2" -> "sum", "f3" -> "", "f4" -> "avg")
    val select2 = Map("f1" -> "", "f5" -> "sum", "f3" -> "", "f6" -> "avg")
    val baseIndexFld = Map("select1" -> select1, "select2" -> select2)

    val index1 = getIndexFromExp(" (tbA.a1 + b.b1)/c.c1")
    val index2 = getIndexFromExp(" (e.a1 + b.b1)*c.c1")
    val testIndex = List(
      ("tt1", " (select1.f3 + select2.f5)/select2.f5"),
      ("tt2", " (select1.a4 + select2.b6)*select2.f5"))
    val indexes = List(index1, index2)
    val indexSet = indexes.foldLeft(Set[String]())((r, e) => {
      r.++(e)
    })
    val res = getAssembleSql(("test", testIndex), baseIndexFld)
    println(res)
  }
}