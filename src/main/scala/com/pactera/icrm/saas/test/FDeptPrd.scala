package com.pactera.icrm.saas.test

import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes

object FDeptPrd {
  val fDeptPrdCfg = (
    "f_dept_prd", Map(
      "prd_no" -> (1, "", ""),
      "prd_type" -> (2, "deptPrdType", ""),
      "dh_flag" -> (3, "deptDhFlg", ""),
      "duration" -> (4, "deptPrdDuration", ""),
      "data_dttm" -> (5, "deptPrdDataDt", "")))

  val prdNo = StructField("deptPrdNo", StringType, true)
  val prdType = StructField("deptPrd001", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val dhFlg = StructField("deptDhFlg", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val duration = StructField("deptPrdDuration", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val dttm = StructField("deptPrdDataDt", DataTypes.createMapType(StringType, DataTypes.StringType), true)

  val row = StructType(List(prdNo, prdType, dhFlg, duration, dttm))
}