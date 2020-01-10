package com.jason.exaples

import org.apache.spark.sql.SparkSession

object RandomSplitTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("random")
      .getOrCreate()
    import spark.implicits._
    val df = (1 to 20).toDF("aa")
    df.show()
    for (i <- 0 to 10) {
      val Array(df1, df2) = df.randomSplit(Array(0.8, 0.2), 1)
      println(df1.count(), df1.collect.mkString(","))
    }
    println("==" * 20)
    for (i <- 0 to 10) {
      val Array(df1, df2) = df.randomSplit(Array(0.8, 0.2))
      println(df1.count(), df1.collect.mkString(","))
    }
    spark.stop()
  }
}
