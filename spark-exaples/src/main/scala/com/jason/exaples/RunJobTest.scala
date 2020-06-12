package com.jason.exaples

import java.io.File
import java.net.URL
import java.util.ServiceLoader

import com.jason.exaples.MysqlExample.spark
import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerInterface}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.util.CompletionIterator

import scala.io.Source

/**
  * 测试sparkcontext 的runjob方法
  */
object RunJobTest {
  val spark = SparkSession.builder()
    .master("local[*]")
    .getOrCreate()
  import spark.implicits._
  def write2mysql(): Unit ={
    val seq  = for(i <- 0 to 1000000)yield ((1,2,3))
    val df = seq.toDF("a","b","c").repartition(8)
    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8")
      .option("dbtable", "databus.runjob")
      .option("user", "root")
      .option("password", "879892206")
      .save()
  }
  def testRunJob(): Unit ={
    val df = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "databus.runjob")
      .option("user", "root")
      .option("password", "879892206")
      .option("fetchsize", "1000")
      .load()
    println("== 开始读取：")
    val star = System.currentTimeMillis()
    df.limit(10).show(false)
    /*val rdd = df.rdd
    val schema = df.schema
    var s = ""
    def row2String:(TaskContext, Iterator[Row]) => String = {
      (tc,it)=>
        val it2 = it.slice(0,10)
        it2.toList.map(_.mkString(",")).mkString("\n")
    }
    val ss = spark.sparkContext.runJob[Row,String](rdd,row2String)
    //val ss = spark.sparkContext.runJob[Row,String](data18.rdd,row2String)
    println(ss.mkString("\n"))*/
    val end = System.currentTimeMillis()
    println(s"耗时：${end-star} ms")
  }
  def main(args: Array[String]): Unit = {
    //write2mysql()
    val cls = Class.forName("com.mysql.jdbc.Driver")
    val uri = cls.getResource("/" + cls.getName.replace('.', '/') + ".class")
    println(uri.toURI)
    val op = SparkContext.jarOfClass(Class.forName("com.mysql.jdbc.Driver"))
    println(op.getOrElse("===="))
    println(new File("/home/jason").toURI)
    import scala.collection.JavaConverters._
    //testRunJob()
    spark.stop()
  }
}
