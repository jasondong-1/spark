package com.jason.exaples

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * 配置表，存储了（配置id，关键关联字段）
  * id1可以看成是name，age。career的hashcod假设hashcode是不重复的e，
  * id1,id2,name,age,career
  *
  * 上一次结果数据
  * 1，jason，30，xx
  * 2，jason，30，xx
  * 3，dong，30，xx
  *
  * 新来的数据
  * sample1：
  * jason，30，xx
  * jason，30，xx
  * jason，30，xx
  * dong，30，xx
  * puma，28，dd
  *
  * sample1存储成功后的数据：
  * 1，jason，30，xx
  * 2，jason，30，xx
  * 3，dong，30，xx
  * 4，puma，28，dd
  * 5，jason，30，xx
  *
  * sample2：
  * jason，30，xx
  * dong，30，xx
  * puma，28，dd
  * lion，27，uu
  **/
object SeriaId extends Logging {

  /**
    * conf 一开始是没有的，要用户指定名称还是支持自动生成表名？
    *
    * @param newDf       输入通道1
    * @param conf        输入通道2
    * @param consecutive Boolean，id是否连续连续增长（partion 连续，partition不连续）
    * @param seed        Long，id基准值，会基于此值递增
    * @param idname      String，id 列的名字
    * @param keys        Array[String],关键关联字段，要从newDf 中选择
    */
  def addSeriaId(spark: SparkSession, newDf: DataFrame, conf: DataFrame,
                 consecutive: Boolean, seed: Option[Long],
                 idname: String, keys: Array[String]): DataFrame = {

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
        conf.select(idnamex).rdd.map(_.getLong(0)).max() + 1
      } else if (seed.isDefined) {
        seed.get
      } else {
        1L
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
    val df21 = spark.sql(s"select -1 as ${idnamex},* from df2")

    val confDf = if (conf != null) {
      //conf 只包括关键字与id字段，需要补齐其他值
      conf
    } else {
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(StructField(idnamex, LongType) +: newDf.schema.fields)
      )
    }

    val df3 = df21.union(confDf)

    //key由用户提供，默认取除id列外的全字段
    val rdd = df3.rdd.groupBy { r =>
      val names = broadKeys.value
      Row(names.map {
        nn =>
          r.get(r.fieldIndex(nn))
      }: _*).mkString(",")
    }

    val rdd2 = rdd.values
      .map { r =>
        val list = r.toList
        val oldData = list.filter(_.getLong(0) != -1L)
        val newData = list.filter(_.getLong(0) == -1L)
        /*println("========old=========")
        println(oldData.mkString(","))
        println("========old=========")
        println("========new=========")
        println(newData.mkString(","))
        println("========new=========")*/
        val zip = newData.zipAll(oldData, null, null).filter {
          case (r1, r2) => r1 != null
        }
        //todo 只替换id值，不是替换整行数据
        zip.map { case (r1, r2) =>
          if (r2 != null) {
            Row((r1.toSeq.updated(0, r2.getLong(0))): _*)
          } else {
            r1
          }
        }
      }

    val rdd3 = rdd2.flatMap(_.toList)
    val oldDataRdd = rdd3.filter(r => (r.getLong(0)) != -1L)
    val newDataRdd = rdd3.filter(r => (r.getLong(0)) == -1L)

    //添加序号，从0 开始，根据consecutive来生成连续喝不连续的id
    val newDataRddZip = if (consecutive) {
      newDataRdd.zipWithIndex()
    } else {
      newDataRdd.zipWithUniqueId()
    }
    //用 index+maxid 替换 -1
    val newDataRddWithRightId = newDataRddZip.map { case (row, index) => Row((row.toSeq.updated(0, index + maxid.value)): _*) }
    val res = newDataRddWithRightId.union(oldDataRdd)
    spark.createDataFrame(res, confDf.schema)
  }

  /**
    * conf 一开始是没有的，要用户指定名称还是支持自动生成表名？
    *
    * @param newDf       输入通道1
    * @param conf        输入通道2
    * @param consecutive Boolean，id是否连续连续增长（partion 连续，partition不连续）
    * @param seed        Long，id基准值，会基于此值递增
    * @param idname      String，id 列的名字
    * @param keys        Array[String],关键关联字段，要从newDf 中选择
    */
  def addSeriaId2(spark: SparkSession, newDf: DataFrame, conf: DataFrame,
                  consecutive: Boolean, seed: Option[Long],
                  idname: String, keys: Array[String]): (DataFrame, DataFrame) = {

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
        conf.select(idnamex).rdd.map(_.getLong(0)).max() + 1
      } else if (seed.isDefined) {
        seed.get
      } else {
        1L
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
    val df21 = spark.sql(s"select cast(-1 as bigint) as ${idnamex},* from df2")

    val confDf = if (conf != null) {
      //conf 只包括关键字与id字段，需要补齐其他值
      conf
    } else {
      val map = newDf.schema.fieldNames.zip(newDf.schema.fields).toMap
      val structField = broadKeys.value.map(fieldName => map(fieldName))
      spark.createDataFrame(
        spark.sparkContext.emptyRDD[Row],
        StructType(StructField(idnamex, LongType) +: structField)
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
      val (newRowsWithId, newRowsWithoutId, oldRowsWithId) = if (r.getAs[Long](idnamex) == -1L) {
        (u._1, r :: u._2, u._3)
      } else {
        (u._1, u._2, r :: u._3)
      }
      val ziprow = newRowsWithoutId.zip(oldRowsWithId).map {
        case (r1, r2) =>
          Row((r1.toSeq.updated(r1.schema.fieldIndex(idnamex), r2.getAs[Long](idnamex))): _*)
      }
      val size = ziprow.size
      (ziprow ::: newRowsWithId, newRowsWithoutId.drop(size), oldRowsWithId.drop(size))
    }

    def combOp: (ThreeList, ThreeList) => ThreeList = (u, v) => {
      val (newRowsWithId, newRowsWithoutId, oldRowsWithId) = (u._1 ::: v._1, u._2 ::: v._2, u._3 ::: v._3)
      val ziprow = newRowsWithoutId.zip(oldRowsWithId).map {
        case (r1, r2) =>
          Row((r1.toSeq.updated(r1.schema.fieldIndex(idnamex), r2.getAs[Long](idnamex))): _*)
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
    val resDf = spark.createDataFrame(resRdd, StructType(StructField(idnamex, LongType) +: newDf.schema.fields)).persist(StorageLevel.MEMORY_AND_DISK)
    val confFields = confDf.schema.fieldNames
    (resDf, resDf.select(confFields(0), confFields.drop(1): _*))
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName(getClass.getSimpleName)
      .getOrCreate()
    import spark.implicits._
    val df1 = Seq(
      (1L, "jason", 30, "xx"),
      (2L, "jason", 30, "xx"),
      (4L, "jason", 30, "xx"),
      (5L, "jason", 30, "xx"),
      (3L, "dong", 30, "xx")
    ).toDF("id", "name", "age", "career")


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

    spark.stop()
  }
}
