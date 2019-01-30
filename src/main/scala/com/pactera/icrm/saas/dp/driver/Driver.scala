package com.pactera.icrm.saas.dp.driver

import scalikejdbc._
import scalikejdbc.config._
import com.pactera.icrm.saas.dp.dbtable.SysProps
import com.pactera.icrm.saas.dp.dbtable.TableCfg
import com.pactera.icrm.saas.dp.dm.DmCatalog
import com.pactera.icrm.saas.test._
import com.pactera.icrm.saas.dp.base.Transfer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import java.text.Format
import org.apache.spark.sql.SaveMode
import ch.qos.logback.classic.Logger
import org.apache.hadoop.hdfs.tools.GetConf
import org.slf4j.LoggerFactory
import com.pactera.icrm.saas.dp.base.DmJoin
import com.pactera.icrm.saas.dp.base.DmSum
import com.pactera.icrm.saas.dp.base.Transfer

object Driver {
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
    val workdate = "20170222"
    val tenant = "TEN000001"
    val savePath = "output"
    val logger = LoggerFactory.getLogger(getClass);
    DBs.setup('saasdev)
    logger.info("get prop concifg")
    val cfg = SysProps.getMapPorpByIn(List("'spark.hdfs.sdata.dir'", "'spark.master.url.spark'", "'spark.master.url.hdfs'"))
    logger.info(cfg.toString())
    logger.info("create spark context")
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

    val factDict = TableCfg.getFactDict(tenant)
    val interDict = TableCfg.getInterfaceDict(tenant)
    val mapping = TableCfg.getMapping(tenant)

    val tableDict = factDict.map(table =>
      (table._1, table._2.map(field =>
        (field._1, (field._2._1, field._2._2))))) ++ interDict

    logger.debug("table cfg:[" + tableDict.toString() + "]")

    logger.debug("mapping:[" + mapping.toString() + "]")
    logger.debug("dimensionClass:[" + DmCatalog.pairs.toString() + "]")

    val handler = new Transfer(tableDict, mapping, DmCatalog.pairs)
    
    logger.info("prepare functions")
    val tasks = Transfer.prepareTransferTask(handler, mapping)
    logger.debug("tasks:[" + tasks.toString() + "]")

    def getData(name: String) = {
      val dataPath = cfg.get("spark.hdfs.sdata.dir").getOrElse("") + "/" + tenant + "/" + workdate + "/" + name
      logger.debug("input path:[" + dataPath + "]")
      spark.read.parquet(dataPath)
      //      spark.read.parquet(dataPath).sample(true, 1)
    }

    def doTransferTask(source: String, dest: String) {
      tasks.get(dest).map(_.get(source).map(_.map(f => {
        val data = getData(source)
        val rdd = data.rdd.map(f)
        val schama = TableCfg.getFactSchema(tenant, dest)
        val df = schama.map(schema => spark.createDataFrame(rdd, schema))
        val outPath = savePath + "/" + workdate + "/" + dest
        logger.debug("output path:[" + outPath + "]")
        df.map(_.write.mode(SaveMode.Overwrite).parquet(outPath))
      })))
    }

    doTransferTask("iacc_t_pe_dept_info", "f_dept_acc")
    doTransferTask("icst_t_pe_cust_info", "f_cust_info")
    
    
    
    val f_dept_acc = spark.read.parquet(savePath + "/" + workdate + "/f_dept_acc")
    val f_cust_info = spark.read.parquet(savePath + "/" + workdate + "/f_cust_info")
    f_cust_info.createOrReplaceTempView("f_cust_info")
    f_dept_acc.createOrReplaceTempView("f_dept_acc")

    val factRel = factDict.map(table => (table._1, table._2.map(field => (field._1, field._2._3))))
    logger.debug("join config:[" + factRel.toString() + "]")
    val factPk = TableCfg.getFactPkField(tenant)
    logger.debug("fact pk config:[" + factPk.toString() + "]")
//    val dmJoin = new DmJoin(factRel, factPk)
//    val sqlStr = dmJoin.getSql("f_dept_acc")
//    println(sqlStr)
//    val joinres = spark.sql(sqlStr)
//    joinres.createOrReplaceTempView("f_dept_acc")
//    joinres.printSchema()
//    joinres.collect().foreach(println)
    //    val tmp = "select birth from f_dept_acc where birth.years in(1,2,5)"
    //    spark.sql(tmp).collect().foreach(println)
    val index = TableCfg.getBaseIndexes(tenant)
    val factSelect = TableCfg.getIndexFld(tenant)
    val factFilter = TableCfg.getIndexFilter(tenant)
    index.foreach(doSum)

    /*
     * (id,(tableName,filterExp))
     */
    def doSum = (args: (String, (String, String))) => {
      val sql = DmSum.createSumSql(
        args._2._1,
        args._2._2,
        factSelect.get(args._1).getOrElse(Map[String, String]()),
        factFilter.get(args._1).getOrElse(Map[String, (String, String, String)]()))
      val res = sql.map(spark.sql(_))
      val outPath = savePath + "/" + workdate + "/json/" + args._2._2
      res.map(_.write.mode(SaveMode.Overwrite).json(outPath))
      res.map(_.createOrReplaceTempView(args._2._2))
    }

    DBs.closeAll()
    spark.stop()
  }
}