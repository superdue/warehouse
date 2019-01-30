package com.pactera.icrm.saas.dp.driver

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.pactera.icrm.saas.dp.dbtable.TableCfg
import com.pactera.icrm.saas.dp.base.DmSum
import org.apache.spark.sql.SaveMode
import scalikejdbc.config.DBs
import com.pactera.icrm.saas.dp.dbtable.SysProps
import org.apache.hadoop.fs.Path
import com.pactera.icrm.saas.dp.dm.Date
import com.pactera.icrm.saas.dp.base.Transfer
import scala.collection.mutable.Stack
import com.pactera.icrm.saas.dp.base.Comm

class IndexSumDriver(spark: SparkSession,
                     tableCfg: Map[String, String],
                     tenant: String) {
  val logger = LoggerFactory.getLogger(getClass);

  /*
   * [指标名 -> (数据源名,过滤表达式)]
   */
  val index = TableCfg.getHisIndexes(tenant)
  val indexFields = TableCfg.getIndexFld(tenant)
  val indexFilters = TableCfg.getIndexFilter(tenant)
  val dataDir = tableCfg.get("spark.hdfs.mdata.dir").getOrElse("tmp")
  val hdfs = org.apache.hadoop.fs.FileSystem.get(
    new java.net.URI(tableCfg.get("spark.master.url.hdfs").get),
    new org.apache.hadoop.conf.Configuration())

  def doOneSum(indexName: String, workDate: String) {
    val someInd = index.find(_._1 == indexName)
    someInd.map(one => {
      val dataPath = dataDir + one._2._1
      logger.debug("dataPath:" + dataPath)
      val list = hdfs.listStatus(new Path(dataPath))
      val paths = list.map(l => {
        l.getPath.toString().takeRight(8)
      })

      val someFilter = indexFilters.get(one._1)
      val transfer = Transfer.doLine(Date.todo)
      val someFiles = someFilter.map(filter => {
        paths.filter(path => {
          Comm.dateFilter(transfer(path), one._2._2, filter)
        }).map(dataPath + "/" + _ + "/" + tenant)
      })
      someFiles.map(_.map(println))
      someFiles.map(files => {
        if (files.size > 0) {
          logger.debug("files:[" + files.mkString(",") + "]")
          val data = spark.read.parquet(files: _*)
          data.createOrReplaceTempView(one._2._1)
          val savePathsuffix = someFilter.map(datefilter => Date.getDateMark(datefilter)).getOrElse("")
          val savePathPrefix = dataDir + one._1 + "/"
          val savePath = savePathPrefix + savePathsuffix + "/" + tenant
          logger.debug("savePath:[" + savePath + "]")
          doSum(one).map(_.write.mode(SaveMode.Overwrite).parquet(savePath))
        }
      })
    })
  }

  /**
   *
   */
  def doSum = (args: (String, (String, String))) => {
    val sql = DmSum.createSumSql(
      args._2._1,
      "",
      indexFields.get(args._1).getOrElse(Map[String, String]()),
      indexFilters.get(args._1).getOrElse(Map[String, (String, String, String)]()))
    logger.info("spark sql:" + sql)
    val res = sql.map(spark.sql(_))
    res
  }
}

object IndexSumDriver {
  def main(args: Array[String]) {
    /*
     * parse args
     */
    val workDate = "20170531"
    val tenant = "TEN000001"
    val savePath = "output"
    val logger = LoggerFactory.getLogger(getClass);

    logger.info("IndexSumDriver Begin:[" + workDate + "][" + tenant + "]")

    DBs.setup('saasdev)
    val tableCfg = SysProps.getMapPorpByIn(
      List(
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
    val driver = new IndexSumDriver(spark, tableCfg, tenant)
    //    val tst = " C&(A|B)&(D|E)"
    //    println(infix2postfix(tst))
    val df1 = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/s22/20170221/TEN000001")
    df1.printSchema()
    df1.collect().foreach(println)
    val df2 = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/s22/20170222/TEN000001")
    df2.printSchema()
    df2.collect().foreach(println)
    driver.doOneSum("sum_s22", workDate)
    val df = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/sum_s22/x-x-month=2-x-year=2017/TEN000001")
    df.printSchema()
    df.collect().foreach(println)
    DBs.closeAll()
    spark.stop()
  }
}