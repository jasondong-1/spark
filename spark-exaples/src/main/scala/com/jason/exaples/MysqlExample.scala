package com.jason.exaples


import java.io.File

import org.apache.spark.SparkFiles
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import java.io.IOException
import java.lang.reflect.Field
import java.net.URI

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

  def save2Mysql(): Unit = {
    import spark.implicits._
    val seq = for (i <- 1 to 100) yield {
      (i, s"spark${i}")
    }


    val df = seq.toDF("id", "name")

    df.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", "jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "databus.test")
      .option("user", "root")
      .option("password", "879892206")
      .save()
  }

  def loadLibrary(libPath: String): Unit = {
    var lib = System.getProperty("java.library.path")
    val dirs = lib.split(":")
    if (!dirs.contains(libPath)) {
      lib = lib + s":${libPath}"
      System.setProperty("java.library.path", lib)
      val fieldSysPath = classOf[ClassLoader].getDeclaredField("sys_paths")
      fieldSysPath.setAccessible(true)
      fieldSysPath.set(null, null)
    }
  }

  import java.io.IOException
  import java.lang.reflect.Field

  @throws[IOException]
  def addLibraryDir(libraryPath: String): Unit = {
    try {
      val field = classOf[ClassLoader].getDeclaredField("usr_paths")
      field.setAccessible(true)
      val paths = field.get(null).asInstanceOf[Array[String]]
      var i = 0
      while (i < paths.length) {
        println(paths(i))
        if (libraryPath == paths(i)) return
        i += 1
      }
      val tmp = new Array[String](paths.length + 1)
      System.arraycopy(paths, 0, tmp, 0, paths.length)
      tmp(paths.length) = libraryPath
      field.set(null, tmp)
    } catch {
      case e: IllegalAccessException =>
        throw new IOException("Failedto get permissions to set library path")
      case e: NoSuchFieldException =>
        throw new IOException("Failedto get field handle to set library path")
    }
  }

  def main(args: Array[String]): Unit = {
    //mysqlDF()
    //save2Mysql()
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "databus.app_aa")
      .option("user", "root")
      .option("password", "879892206")
      .load().printSchema()

    spark.sparkContext.getExecutorMemoryStatus.foreach { case (k, v) => println(k, v) }
    println(spark.sparkContext.getConf.toDebugString)
    spark.sparkContext.addFile("file:/data1/databus_open/workspace/ext_jars/libtaos.so")
    spark.sparkContext.addFile("file:/home/lxadmin/libtaos.so")
    val rdd = spark.sparkContext.range(0, 10, 1, 10).map { l =>
      val so = SparkFiles.get("libtaos.so")
      val parent = new File(so).getParentFile.getAbsolutePath
      loadLibrary(System.getProperty("user.dir"))
      val ia = java.net.InetAddress.getLocalHost
      val host = ia.getHostName //获取计算机主机名
    val IP = ia.getHostAddress //获取计算机IP
      println(host, IP)
      System.loadLibrary("taos")
      s"""${host}<--->${parent}"""
    }
    rdd.collect().foreach(println)

    val rdd2 = spark.sparkContext.range(0, 15, 1, 10).map { l =>
      val ia = java.net.InetAddress.getLocalHost
      val host = ia.getHostName
      s"""${host}----> ${System.getProperty("sun.jnu.encoding")}"""
    }
    rdd2.collect().foreach(println)

    val rdd4 = spark.sparkContext.range(0, 15, 1, 10).map { l =>
      val file = new File("./")
      s"""${file.listFiles().map(_.getName).mkString(",")}"""
    }
    rdd4.collect().foreach(println)


    val rdd3 = spark.sparkContext.range(0, 10, 1, 1).map { l =>
      "hello"
    }

    rdd3.collect().foreach(println)
    System.load("/home/meepo/libtaos.so")
    System.loadLibrary("taos")
    println()
    System.loadLibrary("taos")
    import scala.collection.JavaConverters._
    for((k,v)<-System.getProperties.stringPropertyNames().asScala.map(key => (key, System.getProperty(key))).toMap if k.startsWith("sun")){
      println(s"$k -> $v")
    }
    spark.stop()
  }
}
