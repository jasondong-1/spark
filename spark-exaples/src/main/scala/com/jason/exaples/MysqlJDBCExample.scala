package com.jason.exaples

import java.sql.{DriverManager, PreparedStatement, ResultSet}

object MysqlJDBCExample {
  def main(args: Array[String]): Unit = {
    Class.forName("com.mysql.jdbc.Driver")
    val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/databus?useUnicode=true&characterEncoding=utf-8","root","879892206")
    var ps: PreparedStatement = null
    var rs:ResultSet = null;
    try{
      val start = System.currentTimeMillis()
      ps = conn.prepareStatement("select * from runjob limit 1")
      rs = ps.executeQuery()
      val stop = System.currentTimeMillis()
      println(s"耗时：${stop-start} ms")
    }catch {
      case e:Throwable => println(e.getMessage)
    }finally {
      if(rs!=null){
        rs.close()
      }
      if(ps !=null){
        ps.close()
      }
      if(conn !=null && !conn.isClosed){
        conn.close()
      }
    }
  }
}
