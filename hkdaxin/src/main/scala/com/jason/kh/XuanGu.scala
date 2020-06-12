package com.jason.kh

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object XuanGu {
  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("xuangu")
    .getOrCreate()
  val df = spark.read
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", true)
    .load("/home/jason/Documents/港股打新/oneyeardata")
    .persist(StorageLevel.MEMORY_AND_DISK)

  val tt = s"(twentygushu*price)"
  val rate = 0.015
  //df.show(false)
  //df.printSchema()
  df.createOrReplaceTempView("df")
  //---------暗盘和开盘都盈利------------
  //不亏本的买卖
  val yl = spark.sql(s"select * from df where anpanzf>=${rate} and kpzf>=${rate}").repartition(1)
  yl.createOrReplaceTempView("yl")

  val buyl = spark.sql(s"select * from df where anpanzf<${rate} or kpzf<${rate}").repartition(1)
  buyl.createOrReplaceTempView("buyl")

  //---------暗盘盈利即可-----------
  val anpanyl = spark.sql(s"select * from df where anpanzf>=${rate}").repartition(1)
  anpanyl.createOrReplaceTempView("anpanyl")

  val anpanbuyl = spark.sql(s"select * from df where anpanzf<${rate}").repartition(1)
  anpanbuyl.createOrReplaceTempView("anpanbuyl")

  //---------------按照行业进行分类，并按照暗盘盈利高低来排序
  val hangyeorder = spark.sql(s"select case when anpanzf > ${rate} then 'yl' else 'buyl' end as xx, * from df order by hangye,anpanzf desc")

  def save(df: DataFrame, pathName: String): Unit = {
    df.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("sep", "\t")
      .save(s"/home/jason/Documents/港股打新/${pathName}")
  }

  def chaogou() {
    val chaogouyl = spark.sql("select * from yl order by chaogou").repartition(1)
    save(chaogouyl, "超购盈利")

    val chaogoubuyl = spark.sql("select * from buyl order by chaogou").repartition(1)
    save(chaogoubuyl, "超购不盈利")

    println(chaogoubuyl.count() + chaogouyl.count())
  }

  def jizi(): Unit = {
    val jiziyl = spark.sql("select * from yl order by jizi").repartition(1)
    save(jiziyl, "集资盈利")

    val jizibuyl = spark.sql("select * from buyl order by jizi").repartition(1)
    save(jizibuyl, "集资不盈利")
    println(jiziyl.count() + jizibuyl.count())
  }

  def anpanzf(): Unit = {
    val anpanzf = spark.sql("select * from yl order by anpanzf").repartition(1)
    save(anpanzf, "暗盘涨幅盈利")

    val anpanzfbu = spark.sql("select * from buyl order by anpanzf").repartition(1)
    save(anpanzfbu, "暗盘涨幅不盈利")
    println(anpanzf.count() + anpanzfbu.count())
  }

  //根据出货规则，暗盘全部出光
  def anpanzf2(): Unit = {
    val anpanzf = spark.sql("select * from anpanyl order by hangye,upperprice").repartition(1)
    save(anpanzf, "暗盘出货涨幅盈利")

    val anpanzfbu = spark.sql("select * from anpanbuyl order by hangye,upperprice").repartition(1)
    save(anpanzfbu, "暗盘出货涨幅不盈利")
    println(anpanzf.count() + anpanzfbu.count())
  }

  def anpanzoushi(): Unit = {
    val anpankai = spark.read
      .format("csv")
      .option("header", "false")
      .option("sep", "\t")
      .schema("code string, anpankaizf double")
      .load("/home/jason/Documents/港股打新/全年统计/code_anpankaipanx")

    anpankai.createOrReplaceTempView("anpankai")
    val df2 = spark.sql("select t1.code as code,anpankaizf as akzf,anpanzf as azf from df t1 join anpankai t2 on t1.code=t2.code order by anpankaizf")
    df2.createOrReplaceTempView("df2")
    //低开低走 高开高走
    val df3 = spark.sql("select * from df2 where (akzf<0 and azf < akzf) or (akzf>0 and azf > akzf) order by akzf,azf")
    //涨
    val df4 = spark.sql("select * from df2 where (azf <= akzf) order by akzf,azf")
    val count = df2.count()

    val df3count = df3.count()
    val df4count = df4.count()
    df4.collect().foreach(r=>println(r.mkString("\t")))
    println(s"砸开咋走 " + df3count.toDouble / count.toDouble)
    println(s"涨 " + df4count.toDouble / count.toDouble)
    //save(df2.repartition(1),"暗盘开收盘涨幅")
  }


  def main(args: Array[String]): Unit = {
    //chaogou()
    //jizi()
    //spark.sql("select 1/3 from df").show(false)
    //anpanzf()
    //anpanzf2()
    //save(hangyeorder.repartition(1),"行业排序")
    anpanzoushi
    spark.stop()
  }

}
