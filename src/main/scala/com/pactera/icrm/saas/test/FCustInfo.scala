package com.pactera.icrm.saas.test

import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.DataTypes

object FCustInfo {
  val fCustInfoCfg = (
    "f_cust_info", Map(
      "cust_no" -> (1, "", ""),
      "sex" -> (2, "custSex", ""),
      "org" -> (3, "custOrg", ""),
      "birth" -> (4, "custBirth", ""),
      "marriage" -> (5, "custMarriage", ""),
      "data_dttm" -> (6, "custDataDt", "")))

  val custNo = StructField("custNo", StringType, true)
  val sex = StructField("custSex", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val org = StructField("custOrg", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val birth = StructField("custBirth", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val marriage = StructField("custMarriage", DataTypes.createMapType(StringType, DataTypes.StringType), true)
  val dttm = StructField("custDatadt", DataTypes.createMapType(StringType, DataTypes.StringType), true)

  val row = StructType(List(custNo, sex, org, birth, marriage,dttm))
}