package com.jason.exaples

import org.apache.spark.sql.SparkSession

object UDFTest {
  def test1: Unit = {
    val s =
      s"""
         |add jar /home/meepo/udf/udf.jar;
         |add jar /home/meepo/udf/http-util.jar;
         |
      |CREATE TEMPORARY FUNCTION dp AS 'com.jason.GetDomain';
         |CREATE TEMPORARY FUNCTION base64 AS 'com.jason.Base64x';
         |
      |use paas_ys_g;
         |
      |select base64(word) from demo1 limit 10;
         |select dp('www.baidu.com') from demo1 limit 10;
    """.stripMargin


    val spark = SparkSession.builder().appName("udf").enableHiveSupport().getOrCreate()

    /*spark.sparkContext.addJar("/home/databus/udf.jar")
    spark.sparkContext.addJar("/home/databus/http-util.jar")

    spark.sparkContext.addFile("/home/databus/udf.jar")
    spark.sparkContext.addFile("/home/databus/http-util.jar")*/


    println("============开始添加jar包")
    spark.sql("add jar /home/databus/udf.jar")
    spark.sql("add jar /home/databus/http-util.jar")
    /*spark.sql("CREATE TEMPORARY FUNCTION dp AS 'com.jason.GetDomain'")
    spark.sql("CREATE TEMPORARY FUNCTION base64x AS 'com.jason.Base64x'")
    spark.sql("use kf_hadoop")
    spark.sql("select dp('www.baidu.com') from demo1 limit 10").show()*/
    println("==" * 30)

    import org.apache.spark.SparkFiles
    println(SparkFiles.get("udf.jar"))
    print(spark.sparkContext.master)
    Thread.sleep(500000)
    spark.stop()
  }


  //测试非hive能否使用udf
  def test2: Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("test2")
      .getOrCreate()
    import spark.implicits._
    val df = Seq("a",
      "b",
      "c"
    ).toDF("name")
    df.createOrReplaceTempView("df")
    val s =
      s"""
         |add jar /home/jason/aa/udf.jar;
         |add jar /home/jason/aa/http-util.jar;
         |CREATE TEMPORARY FUNCTION dp AS 'com.jason.GetDomain';
         |CREATE TEMPORARY FUNCTION base64 AS 'com.jason.Base64x';
         |select dp('www.baidu.com') from df limit 10
       """.stripMargin
    spark.sql("add jar /home/jason/aa/udf.jar")
    spark.sql("add jar /home/jason/aa/http-util.jar")
    spark.sql("CREATE TEMPORARY FUNCTION dp AS 'com.jason.GetDomain'")
    spark.sql("CREATE TEMPORARY FUNCTION base64 AS 'com.jason.Base64x'")
    spark.sql("select dp('www.baidu.com') from df limit 10").show()


    spark.stop()
  }


  def main(args: Array[String]): Unit = {
    test2
  }

}