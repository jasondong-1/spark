package com.jason.exaples

import org.apache.spark.sql.SparkSession

object LineCount {
  lazy val spark = SparkSession
    .builder()
    .appName("xx")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      System.err.println("请输入路径！")
      System.exit(1)
    }
    val path = args(0).trim
    val df = spark.read.text(path).toDF("kk")
    val count = df.count()
    val fieldsNum = df.take(100000).map { r =>
      r.getString(0).split("\t", -1).length
    }.distinct
    println(s"""文件： ${path},条数： ${count}, 字段数： ${fieldsNum.mkString("[", ",", "]")}""")
    spark.close()
  }
}
