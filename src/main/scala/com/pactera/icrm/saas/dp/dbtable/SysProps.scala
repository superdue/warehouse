package com.pactera.icrm.saas.dp.dbtable

import java.sql.PreparedStatement
import scalikejdbc._

/* 
 */
object SysProps {
  /*
   * get one prop_val from aplt_t_sys_props by prop_cd
   */
  def getOnePorp(name: String) = {
    val binder = ParameterBinder(
      value = name,
      binder = (stmt: PreparedStatement, idx: Int) => stmt.setString(idx, name))
    NamedDB('saasdev) readOnly { implicit session =>
      sql"select prop_val from aplt_t_sys_props where prop_cd like ${binder}"
        .map(_.string(1)).first.apply()
    }
  }
  def getMapPorpByIn(name: List[String]) = {
    val values = name.mkString("(", ",", ")")
    val sql= "select prop_cd,prop_val from aplt_t_sys_props where prop_cd in " + values
    
    val res = NamedDB('saasdev) readOnly { implicit session =>
      SQL(sql)
        .map(rs => (rs.string(1),rs.string(2))).list.apply()
    }
    res.toMap
  }
  /*
   * get a list of prop_vals from aplt_t_sys_props by prop_cd
   */
  def getMapPorpByLike(name: String) = {
    val binder = ParameterBinder(
      value = name,
      binder = (stmt: PreparedStatement, idx: Int) => stmt.setString(idx, name))
    val res = NamedDB('saasdev) readOnly { implicit session =>
      sql"select prop_cd,prop_val from aplt_t_sys_props where prop_cd like ${binder}"
        .map(res => (res.string(1), res.string(2))).list.apply()
    }
    res.toMap
  }
}