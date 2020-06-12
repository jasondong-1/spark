package com.jason.exaples

import com.jason.exaples.MysqlExample.spark
import org.apache.spark.sql.SparkSession

/**
  * 探索limit语句的谓词下推
  */
object LimitTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("limit")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    import org.apache.spark.sql.functions._
    val df = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "databus.app_metrics")
      .option("user", "root")
      .option("password", "879892206")
      .load()
    df.explain()
    df.select(max("start_time").as("start_time")).join(df,"start_time").show()
    println("="*100)
    df.explain(true)
    val limitDf = df.limit(1)
    println("*"*100)
    limitDf.explain(true)
    limitDf.collect().foreach(r=>println(r.mkString(",")))
    spark.stop()
  }
}
