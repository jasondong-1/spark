package com.jason.exaples

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ArchiveAndFileTest {
  val spark = SparkSession.builder().appName("jjjjjjjjjj").getOrCreate()
  //val path = "hdfs://ns21/user/kf_hadoop/private/demo/1576578688751/ip_line_file"
  val path = "private/aa"
  val rdd = spark.sparkContext.textFile(path)
  spark.sparkContext.addFile("/home/meepo/aa")
  def archives(pp: String): Unit = {
    rdd.mapPartitions { it =>
      val ss = Source.fromFile(pp).getLines().next()
      val buffer = new ArrayBuffer[String]()
      while (it.hasNext) {
        buffer += (it.next() + ss)
      }
      buffer.iterator
    }.saveAsTextFile("private/jjyy")
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("需要传入路径：")
      System.exit(1)
    }
    archives(args(0))
  }

}
