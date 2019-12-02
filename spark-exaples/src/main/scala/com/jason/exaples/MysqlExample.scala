package com.jason.exaples


import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class Record(col1: Int, col2: String, col3: Int)

object MysqlExample {
  lazy val spark = SparkSession
    .builder()
    .master("local")
    .appName("mysql")
    .getOrCreate()

  //spark.sparkContext.setLogLevel("info")
  def mysqlDF(): Unit = {
    import spark.implicits._
    val jdbccdf = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/jason?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "jason.test")
      .option("user", "root")
      .option("password", "123456")
      .option("fetchsize", "1000")
      .load()
    jdbccdf.show()
    jdbccdf.select(($"id" + 1).alias("id"), $"name", $"age")
      .write.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/jason?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "jason.test")
      .option("user", "root")
      .option("password", "123456")
      .option("batchsize", "3")
      .option("logError", "true")
      .option("errorDbtable", "databus_error")
      .option("errorId", "jason2342342")
      .mode(SaveMode.Append)
      .save()
  }

  def hiveDF(): Unit = {
    val hivedf = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:hive2://ddp-jsapp-d013:21060/;principal=impala/ddp-jsapp-d013@EXAMPLE.COM")
      .option("driver", "org.apache.hive.jdbc.HiveDriver")
      .option("dbtable", "g_lx_bus.ydj3fields")
      .load()

    val indf = spark.createDataFrame((1 to 100).map(x => Record(x, s"jason${x}", x)))
    indf.show()

    indf.rdd.map(_.mkString("\t")).saveAsTextFile("hdfs://ns21/user/g_lx_bus/private/hive_table/ydj3fields")
    implicit val rowEncoder = RowEncoder(StructType.fromDDL("aa String"))
    indf.map(s => Row(s.mkString("\t"))).write.mode(SaveMode.Append).text("hdfs://ns21/user/g_lx_bus/private/hive_table/ydj3fields")


    indf.write.format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:hive2://ddp-jsapp-d013:21060/;principal=impala/ddp-jsapp-d013@EXAMPLE.COM")
      .option("driver", "org.apache.hive.jdbc.HiveDriver")
      .option("dbtable", "g_lx_bus.ydj3fields")
      .save()
  }



  def main(args: Array[String]): Unit = {
    mysqlDF()
    spark.stop()
  }
}
