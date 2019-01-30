package com.pactera.icrm.saas.dp.dbtable

import scalikejdbc._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DataTypes
import scalikejdbc.config.DBs

object TableCfg {
  /*
   * get interface table dictionary
   */
  def getInterfaceDict(tenant: String) = {
    val interTables = NamedDB('saasdev) readOnly { implicit session =>
      //      val sqlstr = "select distinct table_name from aplt_t_inter_detail where tenant_no = '" + tenant + "'"
      val sqlstr = "select distinct table_name from aplt_t_inter_detail"
      SQL(sqlstr).map(_.string(1)).list.apply()
    }
    val interfaces = interTables.map(table => {
      val sqlstr = "select column_name,column_index from aplt_t_inter_detail where table_name = '" +
        //      val sqlstr = "select column_name,column_index from aplt_t_inter_detail where tenant_no = '" + tenant + "' and table_name = '" +
        table + "'"
      val fields = NamedDB('saasdev) readOnly { implicit session =>
        SQL(sqlstr).map(rs => (rs.string(1), (rs.int(2), ""))).list.apply()
      }
      (table, fields.toMap)
    })
    interfaces.toMap
  }

  /*
   * return:Map(表名->Map(字段名 -> (字段序号, 维度处理类, 关联表名称)))
   */
  def getFactDict(tenant: String) = {
    val factTables = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select distinct fact_name from aplt_t_facts where tenant_no = '" + tenant + "'"
      SQL(sqlstr).map(_.string(1)).list.apply()
    }

    val facts = factTables.map(table => {
      val sqlstr = "select field_name,field_seq,dimension_class,rel_fact from aplt_t_facts where tenant_no = '" + tenant + "'" + " and  fact_name = '" +
        table + "'"
      val fields = NamedDB('saasdev) readOnly { implicit session =>
        SQL(sqlstr).map(rs => (rs.string(1), (rs.int(2), rs.string(3), rs.string(4)))).list.apply()
      }
      (table, fields.toMap)
    })
    facts.toMap
  }

  /*
   * get get dictionaries of interface tables and facts  
   */
  def getTableDict(tenant: String) = {
    //    (getInterfaceDict(tenant) ++ getFactDict(tenant)).toMap
  }
  /*
   * return:Map(目标表->Map(源表->Map(目标字段，源字段)))	
   */
  def getMapping(tenant: String) = {
    val tables = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select distinct dest_tb,src_tb from aplt_t_mapping where tenant_no = '" + tenant + "'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2))).list.apply()
    }
    /*
     * Map(目标表->list(源表))
     */
    val tableMap = tables.groupBy(_._1).map(x => (x._1, x._2.map(_._2)))
    val mapping = tableMap.map(table => {
      val oneSrcMap = table._2.map(src => {
        val sqlstr = "select dest_fld,src_fld from aplt_t_mapping where tenant_no = '" + tenant + "'" + " and  dest_tb = '" +
          table._1 + "' and src_tb = '" + src + "'"
        val fields = NamedDB('saasdev) readOnly { implicit session =>
          SQL(sqlstr).map(rs => (rs.string(1), rs.string(2))).list.apply()
        }
        (src, fields.toMap)
      })
      (table._1, oneSrcMap.toMap)
    })
    mapping
  }

  /*
   * get fact schema
   */
  def getFactSchema(tenant: String, name: String): Option[StructType] = {
    val fields = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select field_name,value_type from aplt_t_facts where tenant_no = '" + tenant + "' and fact_name = '" + name + "' order by field_seq"
      SQL(sqlstr).map(res => (res.string(1), res.string(2))).list.apply()
    }
    val schema = fields.map(field => {
      field._2 match {
        case "String"    => StructField(field._1, StringType, true)
        case "Double"    => StructField(field._1, DoubleType, true)
        case "Int"       => StructField(field._1, IntegerType, true)
        case "Dimension" => StructField(field._1, DataTypes.createMapType(StringType, DataTypes.StringType), true)
        case _           => StructField(field._1, StringType, true)
      }
    })
    if (schema.size > 0)
      Some(StructType(schema))
    else None
  }
  /*
   * return:Map(表名称->主键字段名称)
   * get Pk pairs 
   */
  def getFactPkField(tenant: String) = {
    val fields = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select fact_name,field_name from aplt_t_facts where tenant_no = '" + tenant + "' and field_seq = 1"
      SQL(sqlstr).map(res => (res.string(1), res.string(2))).list.apply()
    }
    fields.toMap
  }
  /*
   */
  def getIndexFilter(tenant: String) = {
    val filterList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,filter_name,field_name,oper_name,oper_value from aplt_t_index_filter where tenant_no = '" + tenant + "'"
      SQL(sqlstr).map(res =>
        (res.string(1), res.string(2), res.string(3), res.string(4), res.string(5))).list.apply()
    }
    /*
     * Map[id,Map[field,(operation,value)]]
     */
    val filterMap = filterList.groupBy(_._1).map(x =>
      (x._1, x._2.map(y =>
        (y._2, (y._3, y._4, y._5))).toMap))
    filterMap
  }

  /*
   * output:
   * 	Map(指标名称 -> Map(过滤器名称->(操作字段名称，操作符，操作值)))
   */
  def getIndexFilter(indexes: List[String],
                     tenant: String) = {
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,filter_name,field_name,oper_name,oper_value from aplt_t_index_filter where tenant_no = '" +
        tenant + "' and index_name in " + indexes.mkString("('", "','", "')")
      SQL(sqlstr).map(res =>
        (res.string(1), res.string(2), res.string(3), res.string(4), res.string(5))).list.apply()
    }
    val filterMap = fieldList.groupBy(_._1).map(x =>
      (x._1, x._2.map(y =>
        (y._2, (y._3, y._4, y._5))).toMap))
    filterMap
  }

  /*
   * output:
   * 	Map(index_name -> Map(field_name -> api_name))
   */
  def getIndexFld(tenant: String) = {
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,field_name,api_name from aplt_t_index_fields where tenant_no = '" +
        tenant + "'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3))).list.apply()
    }
    val fieldMap = fieldList.groupBy(_._1).map(x =>
      (x._1, x._2.map(y => (y._2, y._3)).toMap))
    fieldMap
  }
  /*
   * output:
   * 	Map(index_name -> Map(field_name -> api_name))
   */
  def getIndexFld(nameList: List[String], tenant: String) = {
    val names = nameList.mkString("('", "','", "')")
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,field_name,api_name from aplt_t_index_fields where tenant_no = '" +
        tenant + "' and index_name in " + names
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3))).list.apply()
    }
    val fieldMap = fieldList.groupBy(_._1).map(x =>
      (x._1, x._2.map(y => (y._2, y._3)).toMap))
    fieldMap
  }

  /*
   * 获取基础指标集
   * 输出:Map(指标名 -> (数据源名,过滤表达式))
   */
  def getBaseIndexes(tenant: String) = {
    val indexList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,source_name,filter_exp  from aplt_t_indexes where tenant_no = '" +
        tenant + "' and index_type = '1'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3))).list.apply()
    }
    val indexMap = indexList.map(x => (x._1, (x._2, x._3))).toMap
    indexMap
  }

  /*
   * input:
   * 	租户号，指标名称列表
   * output:
   * 	Map(指标类型->List(指标名称))
   */
  def getIndexes(nameList: List[String], tenant: String) =
    {
      val names = nameList.mkString("('", "','", "')")
      val sqlstr = "select index_type ,index_name from aplt_t_indexes where tenant_no = '" +
        tenant + "' and index_name in " + names
      val sqlRes = NamedDB('saasdev) readOnly { implicit session =>
        SQL(sqlstr)
          .map(rs => (rs.string(1), rs.string(2))).list.apply()
      }
      val res = sqlRes.groupBy(_._1).map(r => {
        val list = r._2.map(_._2)
        (r._1, list)
      })
      res
    }
  /*
   * 获取基础指标集
   * 输出:[指标名 -> (数据源名,过滤表达式)]
   */
  def getHisIndexes(tenant: String) = {
    val indexList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,source_name,filter_exp  from aplt_t_indexes where tenant_no = '" +
        tenant + "' and index_type = '2'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3))).list.apply()
    }
    val indexMap = indexList.map(x => (x._1, (x._2, x._3))).toMap
    indexMap
  }

  /*
   * get baseIndex 
   * input:
   * 	tenant,List[index_name]
   * 	
   * output:
   * 	Map[index_name -> List[dimension]]
   */
  def getBaseIndexDmBy(tenant: String,
                       index: List[String]) = {
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name,field_name,api_name from aplt_t_index_fields where tenant_no = '" +
        tenant + "' and index_name in " + index.mkString("('", "','", "')")
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3))).list.apply()
    }
    val fieldMap = fieldList.groupBy(_._1).map(x =>
      (x._1, x._2.map(y => (y._2, y._3)).toMap))
    fieldMap
  }

  def getAllAssemble(tenant: String) = {
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select assemble_name,field_name, field_exp from aplt_t_assemble where tenant_no = '" +
        tenant + "'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3))).list.apply()
    }
    val fieldMap = fieldList.groupBy(_._1).map(x => {
      (x._1, x._2.map(y => (y._2, y._3)))
    })
    fieldMap
  }

  def getAssemble(tenant: String, assembleName: String) = {
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select field_name, field_exp from aplt_t_assemble where tenant_no = '" +
        tenant + "' and assemble_name = '" + assembleName + "'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2))).list.apply()
    }
    (assembleName, fieldList)
  }

  /*
   * output:
   * 	Map(指标名称->List((过滤维度名称，逻辑操作符，操作值)))
   */
  def getAssembleFilter(tenant: String, assembleName: String) = {
    val fieldList = NamedDB('saasdev) readOnly { implicit session =>
      val sqlstr = "select index_name, filter_field,filter_oper,filter_value from aplt_t_assemble_filter where tenant_no = '" +
        tenant + "' and assemble_name = '" + assembleName + "'"
      SQL(sqlstr).map(res => (res.string(1), res.string(2), res.string(3), res.string(4))).list.apply()
    }
    val fieldMap = fieldList.groupBy(_._1).map(x => (x._1, x._2.map(y => (y._2, y._3, y._4))))
    fieldMap
  }

  def main(args: Array[String]) {
    DBs.setup('saasdev)
    DBs.closeAll()
  }
}