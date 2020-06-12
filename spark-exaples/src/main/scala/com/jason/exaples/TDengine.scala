package com.jason.exaples

import java.io.File
import java.sql.{DriverManager, Timestamp}
import java.util.concurrent.TimeUnit

import com.jason.example.Jason
import com.mysql.jdbc.TimeUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.internal.StaticSQLConf
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{SaveMode, SparkSession, SparkSessionExtensions}
import org.apache.spark.util.Utils

/**
  * spark-jdbc for TDengine
  */
object TDengine {
  def test(): Unit = {
    Class.forName("com.taosdata.jdbc.TSDBDriver")
    val conn = DriverManager.getConnection("jdbc:TAOS://192.168.1.151:6030/test", "root", "taosdata")
    try {
      val pstm = conn.prepareStatement("select * from log limit 0")
      val rs = pstm.executeQuery()
      val meta = rs.getMetaData
      val cnt = meta.getColumnCount
      for (i <- 1 to cnt) {
        println(s"${meta.getColumnName(i)}:${meta.getColumnTypeName(i)}")
      }
      rs.close()
      pstm.close()
    } finally {
      conn.close()
    }
  }

  def test2(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("tdengine")
      .getOrCreate()

    import spark.implicits._
    val jdbccdf = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:TAOS://192.168.1.151:6030/log")
      .option("driver", "com.taosdata.jdbc.TSDBDriver")
      .option("dbtable", "log")
      .option("user", "root")
      .option("password", "taosdata")
      .option("fetchsize", "1000")
      .load()
    jdbccdf.show(false)
    println("=" * 20)
    jdbccdf.printSchema()
    jdbccdf.select(($"ts" / 1000000).as("ts"), $"level", $"content", $"ipaddr")

    //jdbccdf.select(($"ts" / 1000000).cast(TimestampType).as("ts"), $"level", $"content", $"ipaddr").show(false)
    jdbccdf.select(($"ts" / 1000000).cast(TimestampType).as("ts"), $"level", $"content", $"ipaddr").show(false)
    jdbccdf.select(($"ts" / 1000000).cast(TimestampType).as("ts"), $"level", $"content", $"ipaddr")
      .write.format("jdbc")
      .option("url", "jdbc:TAOS://192.168.1.151:6030/test")
      .option("driver", "com.taosdata.jdbc.TSDBDriver")
      .option("dbtable", "log2")
      .option("user", "root")
      .option("password", "taosdata")
      .mode(SaveMode.Append)
      .save()
    spark.sparkContext.getConf
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    val ti = new Timestamp(1591022993000L)
    val spark = SparkSession.builder()
      .master("local")
      .config("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .getOrCreate()

    spark.getClass.getDeclaredFields.filter(_.getName.contains("extensions")).foreach(println)


    val field = spark.sparkContext.getClass.getDeclaredField("org$apache$spark$SparkContext$$_conf")
    field.setAccessible(true)
    field.get(spark.sparkContext).asInstanceOf[SparkConf].set("spark.tispark.pd.addresses", "10.4.66.208:2379,10.4.66.209:2379,10.4.66.210:2379")
    val extensionField = spark.getClass.getDeclaredField("extensions")
    extensionField.setAccessible(true)
    val extensions = extensionField.get(spark).asInstanceOf[SparkSessionExtensions]
    val extensionConfClassName = "org.apache.spark.sql.TiExtensions"
    try {
      val extensionConfClass = Class.forName(extensionConfClassName)
      val extensionConf = extensionConfClass.newInstance().asInstanceOf[SparkSessionExtensions => Unit]
      extensionConf(extensions)
    } catch {
      // Ignore the error if we cannot find the class or when the class has the wrong type.
      case e@(_: ClassCastException |
              _: ClassNotFoundException |
              _: NoClassDefFoundError) =>
        e.printStackTrace()
    }


    spark.close()
    //test2()
  }
}

object testx{
  println("hello")
  def say = println("world")
}