package com.jason.exaples

import com.jason.exaples.MysqlExample.spark
import org.apache.spark.sql.SparkSession

/**
  * 探索mysql表编码格式为 gbk 时的情形
  * 没测试成功
  */
object TestSparkMysqlCharset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    //?useUnicode=true&characterEncoding=gbk
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "dababus_gbk.system_dwdm")
      .option("user", "root")
      .option("password", "879892206")
      .load()

    df.show(100, false)
    println(System.getProperty("file.encoding"))
    spark.stop()
  }
}
