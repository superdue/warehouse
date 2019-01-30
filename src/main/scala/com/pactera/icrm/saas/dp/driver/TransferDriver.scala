package com.pactera.icrm.saas.dp.driver

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import com.pactera.icrm.saas.dp.dbtable.TableCfg
import com.pactera.icrm.saas.dp.dbtable.SysProps
import com.pactera.icrm.saas.dp.base.Transfer
import com.pactera.icrm.saas.dp.dm.DmCatalog
import org.apache.spark.sql.SaveMode
import scalikejdbc.config.DBs

class TransferDriver(spark: SparkSession,
                     tableCfg: Map[String, String],
                     tenant: String) {

  val logger = LoggerFactory.getLogger(getClass);

  val factDict = TableCfg.getFactDict(tenant)
  val interDict = TableCfg.getInterfaceDict(tenant)
  val mapping = TableCfg.getMapping(tenant)
  val tableDict = factDict.map(table =>
    (table._1, table._2.map(field =>
      (field._1, (field._2._1, field._2._2))))) ++ interDict

  val handler = new Transfer(tableDict, mapping, DmCatalog.pairs)
  val tasks = Transfer.prepareTransferTask(handler, mapping)

  def doOneTransfer(source: String,
                    dest: String,
                    workDate: String) {
    tasks.get(dest).map(_.get(source).map(_.map(f => {
      val dataPath = tableCfg.get("spark.hdfs.sdata.dir").getOrElse("") +
        "/" + tenant + "/" + workDate + "/" + source
      val data = spark.read.parquet(dataPath)
      val rdd = data.rdd.map(f)
      val schama = TableCfg.getFactSchema(tenant, dest)
      val df = schama.map(schema => spark.createDataFrame(rdd, schema))
      val savePath = tableCfg.get("spark.hdfs.mdata.dir").getOrElse("tmp/") +
         dest + "/" + workDate + "/" + tenant
      logger.info("savePath:" + savePath)
      df.map(_.write.mode(SaveMode.Overwrite).parquet(savePath))
    })))
  }

  def doAllTransfer(workDate: String) {
    tasks.foreach(fact => {
      fact._2.foreach(inter => {
        val source = inter._1
        val dest = fact._1
        val func = inter._2
        func.map(f => {
          val dataPath = tableCfg.get("spark.hdfs.sdata.dir").getOrElse("") +
            "/" + tenant + "/" + workDate + "/" + source
          val data = spark.read.parquet(dataPath)
          val rdd = data.rdd.map(f)
          val schama = TableCfg.getFactSchema(tenant, dest)
          val df = schama.map(schema => spark.createDataFrame(rdd, schema))
          val savePath = tableCfg.get("spark.hdfs.mdata.dir").getOrElse("tmp/") +
            dest + "/" + workDate + "/" + tenant
          logger.info("savePath:" + savePath)
          df.map(_.write.mode(SaveMode.Overwrite).parquet(savePath))
        })
      })
    })
  }
}

object TransferDriver {
  /*
  *  intput:
  *  	arg1:business date [20170201] 
  *   arg2:source table name
  *   arg3:destination table name 
  *   arg4:tenant id
  */
  def main(args: Array[String]) {
    /*
     * parse args
     */
    val workdate = "20170531"
    val tenant = "TEN000001"
    val logger = LoggerFactory.getLogger(getClass);

    logger.info("TransferDriver Begin:[" + workdate + "][" + tenant + "]")

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
      //      .config("spark.sql.shuffle.partitions", 4)
      .getOrCreate()
    //    spark.sparkContext.setLogLevel("ERROR")
    val transfer = new TransferDriver(spark, tableCfg, tenant)
    transfer.doAllTransfer(workdate)
    logger.info("TransferDriver End")
    spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/f_dept_acc/20170531/TEN000001").collect().foreach(println)
    spark.read.parquet("hdfs://120.76.136.131:9000/icrm-saas/mdata/f_cust_info/20170531/TEN000001").collect().foreach(println)
    DBs.closeAll()
    spark.stop()
  }
}