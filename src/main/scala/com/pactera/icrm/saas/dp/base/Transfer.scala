package com.pactera.icrm.saas.dp.base

import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import com.pactera.icrm.saas.test.FCustInfo
import com.pactera.icrm.saas.test.testData
import org.apache.spark.sql.SparkSession
import com.pactera.icrm.saas.dp.dm.Dimension

/*
 * tableCfg:Map(表名称->Map(字段名称，(序号，维度名称)))
 * mapping:Map(目标表->Map(源表->Map(目标字段，源字段)))
 */
class Transfer(tableCfg: Map[String, Map[String, (Int, String)]],
               mapping: Map[String, Map[String, Map[String, String]]],
               dimCfg: Map[String, Dimension]) {

  val transformedMap = mapping.map(transformMapping)

  /*
   * input:
   * 	表名称,字段名称,表配置
   * output：
   * 	不存在返回0
  */
  def columnName2Seq(tb: String, fld: String): Option[Int] = {
    val opt = tableCfg.get(tb)
    opt.map(_.get(fld)).map(_.map(_._1)).getOrElse(None)
  }

  /*
   * input:
   * 	(事实表名称,Map[接口表名称，Map[事实表字段名称，接口表字段名称]])
   * output:
   * 	(事实表名称，Map[接口表名称,Map[(事实表字段序号，对应接口表字段序号)]])
   */
  def transformMapping(m: (String, Map[String, Map[String, String]])) = {
    (m._1, m._2.map(i => {
      (i._1, i._2.map(y => (columnName2Seq(m._1, y._1), columnName2Seq(i._1, y._2))))
    }))
  }

  /*
   * 获取维度处理类
   * input:
   * 	维度名称
   * output：
   * 	字段值计算函数
   */
  def getDimFunc(name: String): Option[Any => Map[String, Any]] = {
    dimCfg.get(name) match {
      case None => None
      case Some(dm) => {
        /*Map[维度属性名称->处理函数]*/
        Some(Transfer.doLine(dm.todo))
      }
    }
  }

  /*
     * input：
     * 	接口表名称，事实表名称
     * output:
     * List[(事实表字段序号，接口表字段序号,接口表字段值计算函数)]
     */
  def getOneMapping(i: String, f: String): Option[List[(Int, Option[Int], Option[(Any => Map[String, Any])])]] = {
    transformedMap.get(f).flatMap(mapping => {
      /*List[(字段序号，计算函数)]*/
      val fact = tableCfg.get(f).flatMap(y => {
        /*List(事实表字段序号,字段类型，维度名称)]*/
        val lt = y.values.toList sortBy (_._1)
        //        println("lt")
        //        lt.map(println)
        Some(lt.map(z => (z._1, getDimFunc(z._2))))
      })
      //      println("facts:")
      //      fact.map(println)
      /*Map[(事实表字段序号->对应接口表字段序号)]*/
      mapping.get(i).flatMap(mp => {
        fact.map(_.map(field => {
          (field._1, mp.get(Some(field._1)).getOrElse(None), field._2)
        }))
      })
    })

  }

  /*
 * input:
 * 	接口表名称，事实表名称
 * return:
 *  行处理函数
 */
  def getLineFunc(interface: String, fact: String): Option[Row => Row] =
    {
      /*List[(事实表字段序号，接口表字段序号,接口表字段值计算函数)]*/
      val mp: Option[List[(Int, Option[Int], Option[(Any => Map[String, Any])])]] = getOneMapping(interface, fact)
      mp match {
        case None => None
        case Some(m) => {
          Some(row =>
            {
              val res = m.foldRight(List[Any]())((cfg, y) =>
                {
                  /*如果没做映射，默认值:null*/
                  cfg._2.map(seq => {
                    m(cfg._1 - 1)._3 match {
                      case None    => y.::(row.getAs(seq - 1))
                      case Some(z) => y.::(z(row(seq - 1)))
                    }
                  }).getOrElse(y.::(null))
                })
              //              Row((res.head,res.drop(0)))
              Row.fromSeq(res)
            })
        }
      }
    }

}


object Transfer {
  def doLine: Map[String, Any => Any] => (Any => Map[String, Any]) = ffs =>
    value => {
      ffs map (x => {
        (x._1, x._2(value))
      })
    }

  /*
   * return:Map(目标表->Map(源表->Option[行处理函数]))
   */
  def prepareTransferTask(
    handler: Transfer,
    mapping: Map[String, Map[String, Map[String, String]]]) = {
    mapping.map(f => {
      (f._1, f._2.map(i => {
        (i._1, handler.getLineFunc(i._1, f._1))
      }))
    })
  }
}
