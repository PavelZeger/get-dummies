package com.zeger.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author Pavel Zeger
 * @since 28/01/2021
 * @note get-dummies
 */
trait SparkSessionSetup extends LoggerSetup {

  /**
   * Apache Spark session configuration and initialization
   */
  lazy val sparkConf: SparkConf = new SparkConf()
    .set("hive.mapred.supports.subdirectories", "true")
    .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    .set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
    .set("spark.files.ignoreCorruptFiles", "true")
    .set("spark.sql.files.ignoreCorruptFiles", "true")
    .set("spark.sql.hive.convertMetastoreParquet", "true")
    .set("spark.sql.catalogImplementation", "hive")
    .set("hive.metastore.connect.retries", "15")
    .set("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "false")

  logger.info(s"Apache Spark configuration was initialized: $sparkConf")
  logger.info(s"Apache Spark configuration: ${sparkConf.getAll.mkString("Array(", ", ", ")")}")

  lazy val sparkSession: SparkSession = SparkSession
    .builder()
    .config(sparkConf)
    .enableHiveSupport()
    .getOrCreate()

  logger.info("Apache Spark Session implicits were imported")
  logger.info(s"Apache Spark session initialized: ${sparkSession.toString}")
  logger.info(s"Apache Spark session configuration info: " +
    s"${sparkSession.sparkContext.getConf.getAll.mkString("Array(", ", ", ")")}")

}
