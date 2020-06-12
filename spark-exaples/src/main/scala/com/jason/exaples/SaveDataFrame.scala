package com.jason.exaples

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

case class P1(age: String, name: String)

case class P2(name: String, age: Int)

object SaveDataFrame {
  lazy val spark = SparkSession
    .builder()
    .appName("save")
    .master("local[*]")
    .getOrCreate()


  lazy val df = {
    import spark.implicits._
    (0 to 100).map(i => P1(i + "i", "jason")).toDF()
  }

  def save2Mysql(df: DataFrame): Unit = {
    df.printSchema()
    df.show()
    df.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", "jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "databus.test")
      .option("user", "root")
      .option("password", "879892206")
      .save()
  }

  def emptyDF = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType.fromDDL("age String,name String"))
  }


  def save2csv(): Unit = {
    df.write.csv("aa")
  }


  def main(args: Array[String]): Unit = {
    //save2Mysql(df)
    save2csv()
    spark.stop()
    /*SSx.ff()
    SSx.ff()
    SSx.ff()*/
  }

}

class SSx() {

  import SSx._
  val a = s
}

object SSx {
  val s: String = {
    println("hello")
    "hello"
  }

  def ff(){}
}
