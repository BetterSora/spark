package cn.njupt.SQLProj

import java.sql.{Connection, PreparedStatement}

import cn.njupt.SQLProj.TopNStatJob.{DayProvinceVideoAccessStat, DayVideoAccessStat}

import scala.collection.mutable.ArrayBuffer

/**
  * 各个维度统计的DAO操作
  */
object StatDAO {
  def insertDayVideoAccessTopN(buffer: ArrayBuffer[DayVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection
      // 设置手动提交
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement("insert into day_video_access_topn_stat(day, cms_id, times) values(?, ?, ?)")

      for (elem <- buffer) {
        pstmt.setString(1, elem.day)
        pstmt.setLong(2, elem.cmiId)
        pstmt.setLong(3, elem.times)

        pstmt.addBatch()
      }

      // 执行批量处理
      pstmt.executeBatch()
      // 手工提交
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  def insertDayProvinceVideoAccessTopN(buffer: ArrayBuffer[DayProvinceVideoAccessStat]): Unit = {
    var connection: Connection = null
    var pstmt: PreparedStatement = null

    try {
      connection = MySQLUtils.getConnection
      // 设置手动提交
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement("insert into day_video_province_access_topn_stat(day, cms_id, province, times, times_rank) values(?, ?, ?, ?, ?)")

      for (elem <- buffer) {
        pstmt.setString(1, elem.day)
        pstmt.setLong(2, elem.cmsId)
        pstmt.setString(3, elem.province)
        pstmt.setLong(4, elem.times)
        pstmt.setInt(5, elem.timesRank)

        pstmt.addBatch()
      }

      // 执行批量处理
      pstmt.executeBatch()
      // 手工提交
      connection.commit()
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(connection, pstmt)
    }
  }

  /**
    * 删除指定日期的数据
    */
  def deleteData(day: String): Unit = {
    val tables = Array("day_video_access_topn_stat",
      "day_video_province_access_topn_stat",
      "day_video_traffics_topn_stat")

    var conn: Connection = null
    var pstmt: PreparedStatement = null

    try {
      conn = MySQLUtils.getConnection
      for (table <- tables) {
        pstmt = conn.prepareStatement(s"delete from $table where day = ?")
        pstmt.setString(1, day)
        pstmt.executeUpdate()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      MySQLUtils.release(conn, pstmt)
    }
  }
}
