package com.pactera.icrm.saas.dp.driver

import org.apache.spark.sql.SparkSession
import com.pactera.icrm.saas.dp.dbtable.TableCfg
import com.pactera.icrm.saas.dp.base.DmSum
import org.apache.spark.sql.SaveMode
import org.slf4j.LoggerFactory
import scalikejdbc.config.DBs
import com.pactera.icrm.saas.dp.dbtable.SysProps

class DmSumDriver(spark: SparkSession,
                  tableCfg: Map[String, String],
                  tenant: String) {
  val logger = LoggerFactory.getLogger(getClass);

  val index = TableCfg.getBaseIndexes(tenant)
  val indexFields = TableCfg.getIndexFld(index.keys.toList,tenant)
  val indexFilters = TableCfg.getIndexFilter(index.keys.toList,tenant)

  val dataDir = tableCfg.get("spark.hdfs.mdata.dir").getOrElse("tmp")

  def doAllSum(workDate: String) {
    logger.debug("index:[" + index.toString() + "]")
    index.foreach(one => {
      logger.debug("doSum:[" + one + "]")
      val dataPath = dataDir + "/" + one._2._1 + "/" + workDate + "/" + tenant
      val data = spark.read.parquet(dataPath)
      data.createOrReplaceTempView(one._2._1)
      logger.debug("data path:" + dataPath)
      val df = doSum(one)
      val savePath = dataDir + "/" + one._1 + "/" + workDate + "/" + tenant
      logger.debug("savePath:[" + savePath + "]")
      df.map(_.write.mode(SaveMode.Overwrite).parquet(savePath))
    })
  }

  def doOneSum(indexName: String, workDate: String) {
    val someInd = index.find(_._1 == indexName)
    someInd.map(ind => {
      logger.debug("doSum:[" + indexName + "]")
      val dataPath = dataDir + "/" + ind._2._1 + "/" + workDate + "/" + tenant
      val data = spark.read.parquet(dataPath)
      data.createOrReplaceTempView(ind._2._1)
      logger.debug("data path:" + dataPath)
      val df = doSum(ind)
      val savePath = dataDir + "/" + ind._1 + "/" + workDate + "/" + tenant
      df.map(_.write.mode(SaveMode.Overwrite).parquet(savePath))
    })
  }

  def doSum = (args: (String, (String, String))) => {
    val sql = DmSum.createSumSql(
      args._2._1,
      args._2._2,
      indexFields.get(args._1).getOrElse(Map[String, String]()),
      indexFilters.get(args._1).getOrElse(Map[String, (String, String, String)]()))
    logger.info("spark sql:" + sql)
    val res = sql.map(spark.sql(_))
    res
  }
}

object DmSumDriver {
  def main(args: Array[String]) {
    /*
     * parse args
     */
    val workDate = "20170531"
    val tenant = "TEN000001"
    val savePath = "output"
    val logger = LoggerFactory.getLogger(getClass);

    logger.info("TransferDriver Begin:[" + workDate + "][" + tenant + "]")

    DBs.setup('saasdev)
    val tableCfg = SysProps.getMapPorpByIn(
      List("'spark.hdfs.sdata.dir'",
        "'spark.master.url.spark'",
        "'spark.hdfs.mdata.dir'",
        "'spark.master.url.hdfs'"))

    logger.info(tableCfg.map(_.toString()).mkString("\n"))

    val spark = SparkSession
      .builder()
      //      .master(cfg.get("spark.master.url.spark").getOrElse("local"))
      .master("local")
      .appName("Driver")
      //      .config("spark.sql.crossJoin.enabled", true)
      //      .config("spark.default.parallelism", 4)
      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //    spark.sparkContext.setLogLevel("ERROR")
//    val f_dept_acc = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/f_dept_acc/20170321/TEN000001")
//    f_dept_acc.printSchema()
//    f_dept_acc.collect().foreach(println)

    val sumer = new DmSumDriver(spark, tableCfg, tenant)
    sumer.doAllSum(workDate)
    logger.info("DmSum End")
    val df1 = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/s11/20170531/TEN000001")
    df1.collect().foreach(println)
    println("sep")
    val df2 = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/s22/20170531/TEN000001")
    df2.collect().foreach(println)
    DBs.closeAll()
    spark.stop()
  }
}