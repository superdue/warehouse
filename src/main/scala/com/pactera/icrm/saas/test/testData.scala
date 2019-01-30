package com.pactera.icrm.saas.test

import com.pactera.icrm.saas.dp.dm._

object testData {
/**
 * 
   * 事实表字典
   * 	事实表名称，fDeptAccCfg = (
    "f_dept_acc", Map(
      "acc_no" -> (1, "", ""),
      "bal" -> (2, "", ""),
      "cust" -> (3, "", "fCustInfo"), //todo
      "prd_no" -> (4, "", "fDeptPrd"),
      "data_dttm" -> (5, "dDataDt", "")))

  val fCustInfoCfg = (
    "f_cust_info", Map(
      "cust_no" -> (1, "", ""),
      "sex" -> (2, "dCustSex", ""),
      "org" -> (3, "dCustOrg", ""),
      "birth" -> (4, "dCustBirth", ""),
      "marriage" -> (5, "dCustMarri", ""),
      "data_dttm" -> (6, "dDataDt", "")))

  val fDeptPrdCfg = (
    "f_dept_prd", Map(
      "prd_no" -> (1, "", ""),
      "prd_type" -> (2, "dDeptPrdType", ""),
      "dh_flag" -> (3, "dDeptDhFlg", ""),
      "duration" -> (4, "dDeptDura", ""),
      "data_dttm" -> (5, "dDataDt", "")))
  /*
   	* 接口表字典
  	* 	接口表名称，接口表字段名称，接口表字段序号
   	*/
  val iDeptAccCfg = ("i_dept_acc", Map(
    "acc_no" -> (1, ""),
    "bal" -> (2, ""),
    "cust_no" -> (3, ""),
    "prd_no" -> (4, ""),
    "dttm" -> (5, "")))
  val iDeptPrdCfg = ("i_dept_prd", Map(
    "prd_no" -> (1, ""),
    "prd_type" -> (2, ""),
    "dh_flag" -> (3, ""),
    "duration" -> (4, ""),
    "dttm" -> (5, "")))
  val iCustInfoCfg = ("i_cust_info", Map(
    "cust_no" -> (1, ""),
    "sex" -> (2, ""),
    "org" -> (3, ""),
    "birth" -> (4, ""),
    "marriage" -> (5, ""),
    "dttm" -> (6, "")))

  /*
     * Mapping
   */
  val mapping = Map(
    "i_dept_acc" -> Map(
      "f_dept_acc" -> Map(
        "acc_no" -> "acc_no",
        "bal" -> "bal",
        "cust" -> "cust_no",
        "prd_no" -> "prd_no",
        "data_dttm" -> "dttm")),
    "i_cust_info" -> Map(
      "f_cust_info" -> Map(
        "cust_no" -> "cust_no",
        "sex" -> "sex",
        "org" -> "org",
        "birth" -> "birth",
        "marriage" -> "marriage",
        "data_dttm" -> "dttm")),
    "i_dept_prd" -> Map(
      "f_dept_prd" -> Map(
        "prd_no" -> "prd_no",
        "prd_type" -> "prd_type",
        "dh_flag" -> "dh_flag",
        "duration" -> "duration",
        "data_dttm" -> "dttm")))

  val dateCfg = (1, List("year", "hyear", "quarter", "month", "date", "week", "day", "hmonth"), Date)
  val dimCfg1 = Map(
     "deptAccDataDt" ->Date, 
    "custDataDt" ->Date, 
    "deptPrdDataDt" -> Date,
    "custBirth" -> Birth,
    "custMarriage" -> Code,
    "deptPrdType" -> Code,
    "custSex" -> Code,
    "deptDhFlg" -> Code,
    "custOrg" -> Code,
    "deptPrdDuration" -> Code
  )
  val dimCfg = Map(
    "deptAccDataDt" -> (1, dateCfg._2, dateCfg._3),
    "custDataDt" -> (2, dateCfg._2, dateCfg._3),
    "deptPrdDataDt" -> (10, dateCfg._2, dateCfg._3),
    "custBirth" -> (3, List("years", "ages"), Birth),
    "custMarriage" -> (4, List("value"), Code),
    "deptPrdType" -> (5, List("value"), Code),
    "custSex" -> (6, List("value"), Code),
    "deptDhFlg" -> (7, List("value"), Code),
    "custOrg" -> (8, List("value"), Code),
    "deptPrdDuration" -> (9, List("value"), Code))

  val i_dept_acc = List(
    List("123456", 12.1, "c112030", "p123", "20161116 09:45:12"),
    List("123457", 12.3, "c111030", "p123", "20160113 10:45:12"),
    List("123458", 11.1, "c122110", "p223", "20160122 10:45:12"),
    List("123459", 10.3, "c121030", "p223", "20170126 09:45:12"))
  val i_dept_prd = List(
    List("p123", "123", "1", "12", "20161116 09:45:12"),
    List("p223", "223", "2", "6", "20161116 09:45:12"))
  val i_cust_info = List(
    List("c112030", "2", "232222", "20001010", "1", "20170211"),
    List("c111030", "1", "132222", "20011010", "2", "20170211"),
    List("c122110", "1", "332222", "20021010", "2", "20170211"),
    List("c121030", "2", "432222", "20031010", "1", "20170211"))

  val dateScope = Map(
    "year" -> ("in", List(2016)),
    "month" -> ("notin", List(11)),
    "date" -> ("notin", List(13)))
  val dateFilterExp = "$year&$month&$date"
  val dateFilter = (dateFilterExp, dateScope)
  val filterExp = "$custSex&$deptPrdType&$deptAccDataDt"
  val filter = ("f_dept_acc", filterExp,
    Map("custSex" -> ("$value", Map("value" -> ("in", List(1)))),
      "deptPrdType" -> ("$value", Map("value" -> ("notin", List(123)))),
      "deptAccDataDt" -> dateFilter))
  def main(args: Array[String]) {
    val factCfg = Map(FCustInfo.fCustInfoCfg, FDeptAcc.fDeptAccCfg, FDeptPrd.fDeptPrdCfg)
    val str = factCfg.map(f => {
      fCustInfoCfg._2.map(x => {
        List("'" + f._1 + "'", "'" + x._1 + "'", x._2._1,"''", "'" + x._2._2 + "'", "'" + x._2._3 + "'").mkString(",")
      })
    })
    str.map(_.map(println))
  }
*/
}