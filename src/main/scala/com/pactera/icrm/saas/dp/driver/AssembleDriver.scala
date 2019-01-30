package com.pactera.icrm.saas.dp.driver

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import com.pactera.icrm.saas.dp.dbtable.TableCfg
import scalikejdbc.config.DBs
import com.pactera.icrm.saas.dp.dbtable.SysProps
import com.pactera.icrm.saas.dp.base.Assemble
import org.apache.spark.sql.SaveMode
import com.pactera.icrm.saas.dp.dm.Date

class AssembleDriver(spark: SparkSession,
                     tableCfg: Map[String, String],
                     tenant: String) {
  val logger = LoggerFactory.getLogger(getClass);

  val allIndexFld = TableCfg.getIndexFld(tenant)

  val dataDir = tableCfg.get("spark.hdfs.mdata.dir").getOrElse("tmp")

  def doAssemble(assembleName: String,
                 workDate: String) {

    val assemble = TableCfg.getAssemble(tenant, assembleName)
    logger.debug("assemble:[" + assemble.toString() + "]")

    val someSqlTodo = Assemble.getAssembleSql(assemble, allIndexFld)
    someSqlTodo.map(todo => {
      val indexes = todo._1
      val sql = todo._2
      logger.debug("spark sql:[" + sql + "]")
      val assembleFilter = TableCfg.getAssembleFilter(tenant, assembleName)
      val indexesTodo = TableCfg.getIndexes(indexes.filter(p => { !assembleFilter.contains(p) }), tenant)
      val someHisIndexesTodo = indexesTodo.filter(p => { p._1 == "2" }).get("2")
      logger.debug("someHisIndexesTodo:[" + someHisIndexesTodo + "]")
      someHisIndexesTodo.map(hisIndexesTodo => {
        val hisIndexFilters = TableCfg.getIndexFilter(hisIndexesTodo, tenant)
        hisIndexFilters.foreach(filter => {
          val indexName = filter._1
          val dateMark = Date.getDateMark(filter._2)
          val dataPath = dataDir + "/" + indexName + "/" + dateMark + "/" + tenant
          logger.debug("load:[" + dataPath + "]")
          spark.read.parquet(dataPath).createOrReplaceTempView(indexName)
        })
      })

      val someNowIndexesTodo = indexesTodo.filter(p => { p._1 == "1" }).get("1")
      logger.debug("someNowIndexesTodo:[" + someNowIndexesTodo + "]")
      someNowIndexesTodo.map(nowIndexes => {
        nowIndexes.foreach(indexName => {
          val dateMark = workDate
          val dataPath = dataDir + "/" + indexName + "/" + dateMark + "/" + tenant
          logger.debug("load:[" + dataPath + "]")
          spark.read.parquet(dataPath).createOrReplaceTempView(indexName)
        })
      })

      assembleFilter.map(filter => {
        val indexName = filter._1
        val filterMap = filter._2.map(x => (x._1, (x._1, x._2, x._3))).toMap
        val dateMark = Date.getDateMark(filterMap)
        val dataPath = dataDir + "/" + indexName + "/" + dateMark + "/" + tenant
        logger.debug("load:[" + dataPath + "]")
        spark.read.parquet(dataPath).createOrReplaceTempView(indexName)
      })
      val savePath = dataDir + "/" + assembleName + "/" + workDate + "/" + tenant
      logger.info("saving:[" + savePath + "]")
      spark.sql(sql).write.mode(SaveMode.Overwrite).parquet(savePath)
    })
  }
}
object AssembleDriver {
  def main(args: Array[String]) {
    /*
     * parse args
     */
    val workDate = "20170222"
    val tenant = "TEN000001"
    val savePath = "output"
    val logger = LoggerFactory.getLogger(getClass);

    logger.info("AssembleDriver Begin:[" + workDate + "][" + tenant + "]")

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

    val df1 = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/s11/20170222/TEN000001")
    val df2 = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/s22/20170222/TEN000001")
    df1.printSchema()
    df1.collect().foreach(println)
    df2.printSchema()
    df2.collect().foreach(println)
    val assembler = new AssembleDriver(spark, tableCfg, tenant)
    assembler.doAssemble("assemb11", workDate)
    val df = spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/assemb11/20170222/TEN000001")
    df.printSchema()
    df.collect().foreach(println)
    logger.info("AssembleDriver End")
    DBs.closeAll()
    spark.stop()
  }
}