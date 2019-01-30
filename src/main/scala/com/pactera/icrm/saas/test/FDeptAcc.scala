package com.pactera.icrm.saas.test

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DataTypes

case class FDeptAcc(deptAccNo: String,
      deptBal: Double,
      custNo:String,
      deptPrdNo:String,
      deptAccDataDt:Map[String,Any])
object FDeptAcc {
  val fDeptAccCfg = (
    "f_dept_acc", Map(
      "acc_no" -> (1, "", ""),
      "bal" -> (2, "", ""),
      "cust" -> (3, "custSex,", "fCustInfo"),
      "prd_no" -> (4, "deptPrdType,", "fDeptPrd"),
      "data_dttm" -> (5, "deptAccDataDt", "")))

  val accNo = StructField("deptAccNo", StringType, true)
  val bal = StructField("deptBal", DoubleType, true)
  val cust = StructField("custNo", StringType, true)
  val prdNo = StructField("deptPrdNo", StringType, true)
  val dttm = StructField("deptAccDataDt",DataTypes.createMapType(StringType, DataTypes.StringType))
  val row = StructType(List(accNo,bal,cust,prdNo,dttm))
}