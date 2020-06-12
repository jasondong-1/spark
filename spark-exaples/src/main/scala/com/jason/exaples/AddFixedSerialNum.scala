package com.jason.exaples

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

class AddFixedSerialNum {
  type Param = (Boolean, Option[BigDecimal], String, Array[String])
  type DTIN = (DataFrame, Any)
  type DTOUT = (DataFrame, DataFrame)

  protected def process(spark: SparkSession, param: Param, dtin: DTIN): Option[DTOUT] = {
    val newDf: DataFrame = dtin._1
    val conf: DataFrame = if (dtin._2.isInstanceOf[DataFrame]) {
      dtin._2.asInstanceOf[DataFrame]
    } else {
      null
    }
    val consecutive: Boolean = param._1
    val seed: Option[BigDecimal] = param._2
    val idname: String = param._3
    val keys: Array[String] = param._4
    //如果conf不存在，则代表是第一次处理数据，必须要求传入confTableName
    if (conf == null) {
      //require(confTableName != null && confTableName.nonEmpty, "输入通道2未连接配置表，程序会生成配置表，请输入表名")
      require(idname != null && idname.nonEmpty, "输入通道2未连接配置表，程序会生成配置表，请输入序号列列名")
    }
    //获取id列的列名,优先取conf中的
    val idnamex = if (conf != null) {
      conf.schema.fieldNames.diff(newDf.schema.fieldNames).apply(0)
    } else {
      idname
    }
    /*
    在此id的基础一增加，因为id从0 开始打得，所以加 1。
    优先从conf中取，再从seed中取，不行再设定为1
     */
    val maxid = spark.sparkContext.broadcast {
      if (conf != null && conf.limit(1).count() > 0) {
        conf.select(idnamex).rdd.map(_.getAs[BigDecimal](0)).max() + 1
      } else if (seed.isDefined) {
        seed.get
      } else {
        BigDecimal(1)
      }
    }
    //val broadSchema = spark.sparkContext.broadcast(newDf.schema)

    /*
    1.先解析conf
    2.1不成立解析keys
    3.2不成立则用所有的字段
     */
    val broadKeys = spark.sparkContext.broadcast {
      if (conf != null) {
        conf.schema.fieldNames.diff(Array(idnamex))
      } else if (keys != null && keys.nonEmpty) {
        keys
      } else {
        newDf.schema.fieldNames
      }
    }
    newDf.createOrReplaceTempView("df2")
    //新数据添加id列，值全为-1
    val df21 = spark.sql(s"select cast(-1 as decimal(38,0)) as ${idnamex},* from df2")

    val confDf = if (conf != null) {
      //conf 只包括关键字与id字段，需要补齐其他值
      conf
    } else {
      val map = newDf.schema.fieldNames.zip(newDf.schema.fields).toMap
      val structField = broadKeys.value.map(fieldName => map(fieldName))
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(StructField(idnamex, DecimalType(38, 0)) +: structField)
      )
    }
    val pairRddNew = df21.rdd.keyBy { r =>
      val names = broadKeys.value
      Row(names.map {
        nn =>
          r.get(r.fieldIndex(nn))
      }: _*).mkString(",")
    }

    val pairRddConf = confDf.rdd.keyBy { r =>
      val names = broadKeys.value
      Row(names.map {
        nn =>
          r.get(r.fieldIndex(nn))
      }: _*).mkString(",")
    }
    val unionPairRdd = pairRddNew.union(pairRddConf)
    /*
    (List.empty[Row],List.empty[Row],List.empty[Row]) 打上id的新纪录，未打上id的新纪录，旧记录
     */
    type ThreeList = (List[Row], List[Row], List[Row])
    val threeList = (List.empty[Row], List.empty[Row], List.empty[Row])

    def seqOp: (ThreeList, Row) => ThreeList = (u: ThreeList, r: Row) => {
      val (newRowsWithId, newRowsWithoutId, oldRowsWithId) = if (r.getAs[java.math.BigDecimal](idnamex) == BigDecimal(-1).bigDecimal) {
        (u._1, r :: u._2, u._3)
      } else {
        (u._1, u._2, r :: u._3)
      }
      val ziprow = newRowsWithoutId.zip(oldRowsWithId).map {
        case (r1, r2) =>
          Row((r1.toSeq.updated(r1.schema.fieldIndex(idnamex), r2.getAs[java.math.BigDecimal](idnamex))): _*)
      }
      val size = ziprow.size
      (ziprow ::: newRowsWithId, newRowsWithoutId.drop(size), oldRowsWithId.drop(size))
    }

    def combOp: (ThreeList, ThreeList) => ThreeList = (u, v) => {
      val (newRowsWithId, newRowsWithoutId, oldRowsWithId) = (u._1 ::: v._1, u._2 ::: v._2, u._3 ::: v._3)
      val ziprow = newRowsWithoutId.zip(oldRowsWithId).map {
        case (r1, r2) =>
          Row((r1.toSeq.updated(r1.schema.fieldIndex(idnamex), r2.getAs[java.math.BigDecimal](idnamex))): _*)
      }
      val size = ziprow.size
      (ziprow ::: newRowsWithId, newRowsWithoutId.drop(size), oldRowsWithId.drop(size))
    }

    val combineRdd = unionPairRdd.aggregateByKey(threeList)(seqOp, combOp).values.map(tp => (tp._1, tp._2)).persist(StorageLevel.MEMORY_AND_DISK)

    val rddWithId = combineRdd.flatMap(_._1)
    val rddWithoutId = combineRdd.flatMap(_._2)


    //添加序号，从0 开始，根据consecutive来生成连续喝不连续的id
    val rddZipWithId = if (consecutive) {
      rddWithoutId.zipWithIndex()
    } else {
      rddWithoutId.zipWithUniqueId()
    }
    //用 index+maxid 替换 -1
    val rddWithRightId = rddZipWithId.map { case (row, index) => Row((row.toSeq.updated(row.schema.fieldIndex(idnamex), index + maxid.value)): _*) }
    val resRdd = rddWithRightId.union(rddWithId)
    val resDf = spark.createDataFrame(resRdd, StructType(StructField(idnamex, DecimalType(38, 0)) +: newDf.schema.fields)).persist(StorageLevel.MEMORY_AND_DISK)
    val confFields = confDf.schema.fieldNames
    Some((resDf, resDf.select(confFields(0), confFields.drop(1): _*)))
  }
}

object AddFixedSerialNum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()
    import spark.implicits._
    val df1 = Seq(
      (BigDecimal(1), "jason", 30, "xx"),
      (BigDecimal(2), "jason", 30, "xx"),
      (BigDecimal(3), "jason", 30, "xx"),
      (BigDecimal(4), "jason", 30, "xx"),
      (BigDecimal(5), "dong", 30, "xx")
    ).toDF("id", "name", "age", "career")
    val sfdf = Seq(
      (156, 1, 15.8, 50548, 500),
      (157, 3, 152.8, 20548, 600),
      (158, 7, 17.8, 3548, 500),
      (159, 3, 15.8, 40548, 560),
      (160, 4, 15.8, 10548, 500),
      (161, 3, 152.8, 70548, 500),
      (162, 12, 15.8, 30548, 504),
      (163, 2, 15.8, 50348, 520),
      (164, 5, 15.8, 50548, 550),
      (165, 6, 15.8, 52548, 580)
    ).toDF("id", "flight_phase", "vib", "flying_hight", "flying_speed")
    sfdf.createOrReplaceTempView("t1")
    spark.sql("select *,row_number() over(partition by flight_phase order by vib desc) as rk  from t1").show()
    spark.sql("select *,rank() over(partition by flight_phase order by vib desc) as rk  from t1").show()
    spark.sql("select *,dense_rank() over(partition by flight_phase order by vib desc) as rk  from t1").show()

    /*sf3.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/databus")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "databus.sf_test")
      .option("user", "root")
      .option("password", "879892206")
      .save()*/
//rank() over(partition by course order by score desc)

    val df2 = Seq(
      ("jason", 30, "xx"),
      ("jason", 30, "xx"),
      ("jason", 30, "xx"),
      ("dong", 30, "xx"),
      ("puma", 28, "dd")
    ).toDF("name", "age", "career").repartition(3)
    /*val (resdf, confdf) = addSeriaId2(spark, df2, null, true, Some(10L), "id", null)
    resdf.show()
    confdf.show()*/
    /*val addFixedSerialNum = new AddFixedSerialNum
    val res = addFixedSerialNum.process(spark, (true, Some(BigDecimal("911010000800002999999999")), "id", null), (df2, null))
    if (res.isDefined) {
      res.get._1.printSchema()
      res.get._1.show(false)
      res.get._2.printSchema()
      res.get._2.show(false)
    }*/
    spark.stop()
  }
}