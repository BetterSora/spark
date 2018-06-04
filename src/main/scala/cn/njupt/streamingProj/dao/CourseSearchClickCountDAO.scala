package cn.njupt.streamingProj.dao

import cn.njupt.streamingProj.HBaseUtils
import cn.njupt.streamingProj.domain.CourseSearchClickCount

import scala.collection.mutable.ListBuffer

/**
  * 从搜索引擎过来的实战课程点击数-数据访问层
  */
object CourseSearchClickCountDAO {
  // 表名
  val tableName: String = "course_search_clickcount"
  // 列族名
  val cf: String = "info"
  // 列名
  val qualifier: String = "click_count"

  /**
    * 保存数据到HBase
    */
  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {
    val hTable = HBaseUtils.getInstance().getTable(tableName)

    list.foreach(courseClickCount => hTable.incrementColumnValue(courseClickCount.day_search_course.getBytes(),
      cf.getBytes(), qualifier.getBytes(), courseClickCount.click_count))
  }
}
