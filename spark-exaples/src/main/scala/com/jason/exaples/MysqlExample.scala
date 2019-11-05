package com.jason.exaples

import org.apache.spark.sql.{SaveMode, SparkSession}

object MysqlExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("mysql")
      .getOrCreate()
    import spark.implicits._
    val jdbccdf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/tai?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "tai.test")
      .option("user", "root")
      .option("password", "123456")
      .option("fetchsize", "1000")
      .load()
    jdbccdf.show()
    jdbccdf.select(($"id" + 1).alias("id"), $"name", $"age")
      .write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/tai?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "tai.test")
      .option("user", "root")
      .option("password", "123456")
      .option("batchsize","1")
      .mode(SaveMode.Append)
      .save()

    spark.stop()
  }
}
