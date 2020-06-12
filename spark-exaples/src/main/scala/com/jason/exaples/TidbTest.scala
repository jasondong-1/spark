package com.jason.exaples

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

object TidbTest {
  val spark = SparkSession.builder()
    .master("yarn")
    .getOrCreate()

  import spark.implicits._
  def readTest() {

  }

  def writeTest(): Unit = {

    val customer = Seq(
      (1,"scala"),
      (1,"java"),
      (1,"python"),
      (1,"php")
    ).toDF("id","code")
    val df = customer.repartition(2)
    df.write
      .mode(saveMode = "append")
      .format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://127.0.0.1:4000/test?rewriteBatchedStatements=true")
      .option("useSSL", "false")
      .option(JDBCOptions.JDBC_BATCH_INSERT_SIZE, 150)
      .option("dbtable", s"cust_test_select") // database name and table name here
      .option("isolationLevel", "NONE") // recommended to set isolationLevel to NONE if you have a large DF to load.
      .option("user", "g_lx_bus") // TiDB user here
      .option("password","5RiwSu29FN3u")
      .save()
  }

  def main(args: Array[String]): Unit = {
    val field = spark.sparkContext.getClass.getDeclaredField("org$apache$spark$SparkContext$$_conf")
    field.setAccessible(true)
    field.get(spark.sparkContext).asInstanceOf[SparkConf].set("spark.tispark.pd.addresses","10.4.66.208:2379,10.4.66.209:2379,10.4.66.210:2379")
    field.get(spark.sparkContext).asInstanceOf[SparkConf].set("spark.sql.extensions","org.apache.spark.sql.TiExtensions")
    spark.sparkContext.addJar("file:/data1/databus_open/workspace/ext_jars/tispark-core-2.1.9-spark_2.3-jar-with-dependencies.jar")
    spark.stop()
  }

}
