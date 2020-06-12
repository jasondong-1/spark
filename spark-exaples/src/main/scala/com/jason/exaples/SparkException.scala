package com.jason.exaples

import java.io.{FileOutputStream, PrintWriter, StringWriter}

import org.apache.hadoop.hive.common.io.CachingPrintStream
import org.apache.spark.sql.SparkSession

object SparkException {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local")
      .getOrCreate()
    val in = new StringWriter()
    val p = new PrintWriter(in)
    try {
      spark.read.format("csv").load("abc")
    } catch {
      case e: Throwable =>
        e.printStackTrace(p)
        p.flush()
        p.close()
    } finally {
      spark.stop()
    }
    import scala.collection.JavaConverters._
    println(in.toString)
  }
}
