package com.jason.exaples

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Person1(name: String, age: Int)

case class Person2(age: Int, name: String)

object CreateDataFrame {
  lazy val spark = SparkSession
    .builder()
    .appName("test")
    .master("local[1]")
    .config("aa", "bb")
    .getOrCreate()


  def mysqlDF(): Unit = {
    val jdbccdf = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/jason?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "jason.test")
      .option("user", "root")
      .option("password", "123456")
      .option("fetchsize", "1000")
      .load()
    jdbccdf.show(20, true)
  }

  def csvDF(): Unit = {
    val csvdf = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "false")
      .schema("name string, age int")
      .load("data/person.csv")
    val bb = spark.sparkContext.getConf.get("aa")
    println(bb)
    csvdf.show()
  }

  def textDF(): Unit = {
    val textdf = spark.read.text("data/person.csv")
    textdf.show()
  }

  def test(): Unit = {
    val it = List(1, 2, 3).iterator
    for (i <- 0 to 3) {
      println(it.hasNext)
    }

  }

  def testEqSchema(): Unit = {
    import spark.implicits._
    val list = 0 to 100
    val schema1 = list.map(i => Person1("jason", i)).toDF().schema
    val schema2 = list.map(i => Person2(i, "jason")).toDF().schema
    println(s"""schema1 : ${schema1.toString()}""")
    println(s"""schema2 : ${schema2.toString()}""")
    println(s"""schema1 == schema2 ${schema1 == schema2}""")
    println(schema1.sql)
  }

  def create(): DataFrame = {
    {
      import scala.io.Source
      val reader = Source.fromFile("arch.zip/arch/aa/aa.txt").getLines()
      while(reader.hasNext){
        println(reader.next())
      }
      import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
      SparkFiles.get("arch")
      import spark.implicits._
      val df = (0 to 100).map {
        i => ("jason", "vf", i)
      }.toDF("col1", "col2", "col3")
      df
    }
  }

  def main(args: Array[String]): Unit = {
    //mysqlDF()
    //csvDF()
    //textDF()
    //println(List("cc.aa","bb.aa","aa.bb").sorted)
    //testEqSchema()
    create().show()
  }

}
