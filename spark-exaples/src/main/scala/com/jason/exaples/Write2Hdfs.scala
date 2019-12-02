package com.jason.exaples

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.SparkSession

object Write2Hdfs {

  def tp()={
    (1,"s")
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("hdfs")
      .getOrCreate()
    val config = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(config)
    val arr = fs.listStatus(new Path("hdfs://ns21/user/s_it_ics/"))
    val it = config.iterator()
    while(it.hasNext){
      val entry = it.next()
      val key = entry.getKey
      if(key.contains("kerberos")){
        println(s"${key} : ${entry.getValue}")
      }
    }
    UserGroupInformation.getCurrentUser.getShortUserName

    for(filestatus <- arr){
      val permission = filestatus.getPermission
      println(s"""${filestatus.getPath}  ${filestatus.getOwner} ${filestatus.getGroup} ${permission}""")
    }


    val out = fs.create(new Path(""))
    out.flush()
    out.close()


    spark.stop()

  }

}
