package com.zeger.util

import org.apache.commons.lang.SystemUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
 * @author Pavel Zeger
 * @since 28/01/2021
 */
trait SparkSessionSetupTest extends FunSuite with BeforeAndAfterAll with LoggerSetup {

  /**
   * Windows or Unix configuration for Hive directories
   */
  val osName: String = SystemUtils.OS_NAME.split("\\s").toList.head.trim
  val warehouseLocation: String = osName match {
    case "Windows" => "file:\\C:\\warehouse"
    case _ => "/usr/spark/warehouse"
  }
  val hiveScratchDir: String = osName match {
    case "Windows" => "C:\\tmp\\hive"
    case _ => "/tmp/hive"
  }

  lazy val sparkConf: SparkConf = new SparkConf()
    .set("spark.master", "local[*]")
    .set("spark.submit.deployMode", "client")
    .set("hive.exec.scratchdir", hiveScratchDir)
    .set("hive.metastore.warehouse.dir", warehouseLocation)
    .set("spark.sql.warehouse.dir", warehouseLocation)
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .set("spark.sql.catalogImplementation", "hive")
    .set("hive.metastore.connect.retries", "15")
    .set("spark.debug.maxToStringFields", "50")
    .set("hive.mapred.supports.subdirectories", "true")
    .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "false")

  logger.info(s"Apache Spark configuration was initialized: $sparkConf")

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .appName("Test")
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  logger.info(s"Apache Spark session was initialized: $sparkSession")
  logger.info(s"Apache Spark session configuration info: " +
    s"${sparkSession.sparkContext.getConf.getAll.mkString("Array(", ", ", ")")}")

  override def afterAll(): Unit = {
    sparkSession.stop()
    logger.info("Apache Spark session was stopped")
  }

  lazy val sparkContext: SparkContext = SparkContext.getOrCreate(sparkConf)

}
