package com.jason.exaples

import java.sql.Date

import com.jason.exaples.MysqlExample.spark
import org.apache.spark.sql.SaveMode

object PostgreTest {
  def main(args: Array[String]): Unit = {
    val df = spark.read.format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://192.168.1.151:5432/resource_db")
      .option("dbtable", "resource_db.ctzy.bas_county_pg")
      .option("user", "root")
      .option("password", "Root@123")
      .load()
    df.printSchema()
    df.show(false)
    import spark.implicits._

    val df2 = Seq(
      ( "dong", 31, "hello", 12.0f, new java.sql.Date(100, 1, 5))
    ).toDF( "name", "age", "address", "salary", "join_date")
    df2.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://192.168.1.151:5432/test?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "company")
      .option("user", "postgres")
      .option("password", "123456")
      .save()
    spark.stop()
  }
}
