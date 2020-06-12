package com.jason.kh

import java.io.PrintWriter

import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.io.Source

object Analyze {
  lazy val spark = SparkSession
    .builder()
    .appName("hk")
    .master("local[*]")
    .getOrCreate()

  def aastockLivermore(): Unit = {
    val aastock = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", false)
      .schema(
        "code string,name string,lowerprice string,upperprice string,price string,num string,onehandnum string")
      .load("/home/jason/Documents/港股打新/全年统计/aastock分析")
    aastock.createOrReplaceTempView("aastock")
    val livermore = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", false)
      .schema(
        "code string,name string,bankuai string,hangye string,date string,overbooking string,Lotteryrate string")
      .load("/home/jason/Documents/港股打新/全年统计/livermore分析")
    livermore.createOrReplaceTempView("livermore")
    val spj = spark.read
      .format("csv")
      .option("sep", "\t")
      .option("header", false)
      .schema(
        "code string,spj string")
      .load("/home/jason/Documents/港股打新/全年统计/code_spjx")
    spj.createOrReplaceTempView("spj")

    val df = spark.sql(
      s"""
         |select l.code,l.name,bankuai,hangye,date,lowerprice,upperprice,price,num,"" as a,onehandnum,"" as b,"" as c,overbooking,"" as d,Lotteryrate,"" as  e, spj, "" as f,"" as g,"" as h,"" as i,"" as j
         |from aastock a join livermore l join spj s
         |on a.code=l.code and l.code=s.code
       """.stripMargin)
    /*    val df = spark.sql(
          s"""
             |select l.code
             |from aastock a join livermore l
             |on a.code=l.code
           """.stripMargin)*/
    df.show(false)
    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", false)
      .option("sep", "\t")
      .save("/home/jason/Documents/港股打新/全年统计/res")
    spark.stop()
  }

  def getspj(): Unit = {
    val regex = ".*(收盘|收报)[^\\d\\.]*(\\d+.?\\d+).*".r
    val lines = Source.fromFile("/home/jason/Documents/港股打新/全年统计/sp").getLines()
    val out = new PrintWriter("/home/jason/Documents/港股打新/全年统计/sp2")
    try {
      for (line <- lines) {
        val arr = line.split("\t", 3);
        if (arr.size != 3) {
          println(line)
        }
        arr(2) match {
          case regex(a, b) => arr(1) = b
          case a =>
        }
        out.println(arr.mkString("\t"))
        out.flush()
      }
    } finally {
      out.close()
    }

  }

  def code_spj(srcFile: String, destFile: String): Unit = {
    val lines = Source.fromFile(s"/home/jason/Documents/港股打新/全年统计/${srcFile}").getLines()
    val out = new PrintWriter(s"/home/jason/Documents/港股打新/全年统计/${destFile}")
    try {
      val nonEmptyLines = lines.filter { s =>
        val arr = s.split("\t", 2)
        arr.forall(!_.isEmpty)
      }
      //println(nonEmptyLines.size)
      for (line <- nonEmptyLines) {
        out.println(line)
      }
    } finally {
      out.flush()
      out.close()
    }


  }

  def kpj: Unit = {
    val codes = Source.fromFile("/home/jason/Desktop/codex").getLines()
    val code_kpj = Source.fromFile("/home/jason/Desktop/kpj2").getLines()
    val arr = for (line <- code_kpj) yield {
      val arr = line.split("\t", -1)
      if (arr.length != 3) {
        println(line)
      }
      (arr(0), arr(2))
    }
    val map = arr.toMap
    val out = new PrintWriter("/home/jason/Desktop/kpj3");
    try {
      for (line <- codes) {
        out.println(s"""${line}\t${map.getOrElse(line, "")}""")
      }
    } finally {
      out.flush()
      out.close()
    }


  }

  def main(args: Array[String]): Unit = {
    //getspj()
    //aastockLivermore()
    //code_spj("code_spj","code_spjx")
    code_spj("code_anpankaipan","code_anpankaipanx")
    //kpj
  }
}
