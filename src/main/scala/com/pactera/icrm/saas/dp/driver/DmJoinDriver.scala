package com.pactera.icrm.saas.dp.driver

import org.slf4j.LoggerFactory
import com.pactera.icrm.saas.dp.dbtable.SysProps
import com.pactera.icrm.saas.dp.dbtable.TableCfg
import org.apache.spark.sql.SparkSession
import com.pactera.icrm.saas.dp.base.DmJoin
import scalikejdbc.config.DBs
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.fs.Path

class DmJoinDriver(spark: SparkSession,
                   tableCfg: Map[String, String],
                   tenant: String) {
  val logger = LoggerFactory.getLogger(getClass);

  val factDict = TableCfg.getFactDict(tenant)
  val factRel = factDict.map(table => (table._1, table._2.map(field => (field._1, field._2._3))))
  val factPk = TableCfg.getFactPkField(tenant)
  val hdfs = org.apache.hadoop.fs.FileSystem.get(
    new java.net.URI(tableCfg.get("spark.master.url.hdfs").get),
    new org.apache.hadoop.conf.Configuration())

  def doDmJoin(fact: String, workDate: String) {
    val someTable = DmJoin.getDmJoinTable(fact, factRel)
    val someSql = DmJoin.getDmJoinSql(fact, factRel, factPk)
    (someTable, someSql) match {
      case (Some(tables), Some(sql)) => {
        val dataDir = tableCfg.get("spark.hdfs.mdata.dir").getOrElse("tmp")
        logger.debug("tables:" + tables.mkString("\n"))
        val dataPath = dataDir + "/" + fact + "/" + workDate + "/" + tenant
        val hdfsPath = new Path(dataPath)
        if (hdfs.exists(hdfsPath)) {
          logger.debug("rename  [" + hdfsPath + "]")
          hdfs.rename(hdfsPath, new Path(dataPath + ".bak"))
        }
        val data = spark.read.parquet(dataPath + ".bak")
        data.createOrReplaceTempView(fact)
        tables.foreach(table => {
          val dataPath = dataDir + "/" + table + "/" + workDate + "/" + tenant
          spark.read.parquet(dataPath).createOrReplaceTempView(table)
        })
        logger.info("spark sql:" + sql)
        spark.sql(sql).write.mode(SaveMode.Overwrite).parquet(dataPath)
        logger.info("saved:" + dataPath)
      }
      case _ => Unit
    }
  }
}
object DmJoinDriver {
  def main(args: Array[String]) {
    val workDate = "20170531"
    val tenant = "TEN000001"
    val savePath = "output"
    val logger = LoggerFactory.getLogger(getClass);

    logger.info("DmJoinDriver Begin:[" + workDate + "][" + tenant + "]")

    DBs.setup('saasdev)

    val tableCfg = SysProps.getMapPorpByIn(
      List(
        "'spark.master.url.spark'",
        "'spark.master.url.hdfs'",
        "'spark.hdfs.mdata.dir'"))

    logger.info(tableCfg.map(_.toString()).mkString("\n"))

    val spark = SparkSession
      .builder()
      //      .master(cfg.get("spark.master.url.spark").getOrElse("local"))
      .master("local")
      .appName("Driver")
      //      .config("spark.sql.crossJoin.enabled", true)
      //      .config("spark.default.parallelism", 4)
      //      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //    spark.sparkContext.setLogLevel("ERROR")
    val join = new DmJoinDriver(spark, tableCfg, tenant)
    join.doDmJoin("f_dept_acc", workDate)
    logger.info("DmJoinDriver End")
    spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/f_dept_acc/20170531/TEN000001").collect().foreach(println)
    DBs.closeAll()
    spark.stop()
  }
}