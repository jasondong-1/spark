package com.jason.exaples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
object DecimalTest {
  def main(args: Array[String]): Unit = {
   val spark = SparkSession.builder()
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1,Decimal(BigDecimal(1),10,2))
    ).toDF("id","de")

    df.printSchema()

    spark.stop()
  }
}
