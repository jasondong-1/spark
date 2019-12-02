package com.jason.exaples

import org.apache.spark.sql.SparkSession

object WordCount {
  def printFGF(name:String): Unit = {
    println(s"""${"="*25}${name}${"="*25}""")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("wc")
      .getOrCreate()
    val sc = spark.sparkContext
    printFGF("conf")
    sc.getConf.getAll.foreach(println)
    printFGF("lines")
    val lines = sc.textFile("README.md", 2)
    printFGF("words")
    val words = lines.flatMap(line => line.split("\\s+"))
    printFGF("ones")
    val ones = words.map(word => (word, 1))
    printFGF("counts")
    val counts = ones.reduceByKey(_ + _)
    printFGF("foreach")
    counts.foreach(println)
    printFGF("sleep")
    Thread.sleep(500000)
    spark.stop()
  }
}
