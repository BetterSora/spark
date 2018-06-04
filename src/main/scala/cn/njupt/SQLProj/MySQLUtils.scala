package cn.njupt.SQLProj

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Mysql工具类
  */
object MySQLUtils {
  /**
    * 获取MySQL连接
    */
  def getConnection: Connection = {
    //DriverManager.getConnection("jdbc:mysql://localhost:3306/SQLProject", "root", "root")
    DriverManager.getConnection("jdbc:mysql://hadoop:3306/SQLProject?useUnicode=true&characterEncoding=utf8", "root", "root")
  }

  /**
    * 释放MySQL连接
    */
  def release(connection: Connection, pstmt: PreparedStatement): Unit = {
    try {
      if (pstmt != null) {
        pstmt.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if (connection != null) {
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(getConnection)
  }
}
