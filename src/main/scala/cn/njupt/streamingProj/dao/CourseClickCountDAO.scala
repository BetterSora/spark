package cn.njupt.streamingProj.dao

import cn.njupt.streamingProj.HBaseUtils
import cn.njupt.streamingProj.domain.CourseClickCount

import scala.collection.mutable.ListBuffer

/**
  * 实战课程点击数-数据访问层
  */
object CourseClickCountDAO {
  // 表名
  val tableName: String = "course_clickcount"
  // 列族名
  val cf: String = "info"
  // 列名
  val qualifier: String = "click_count"

  /**
    * 保存数据到HBase
    */
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val hTable = HBaseUtils.getInstance().getTable(tableName)

    list.foreach(courseClickCount => hTable.incrementColumnValue(courseClickCount.day_course.getBytes(),
      cf.getBytes(), qualifier.getBytes(), courseClickCount.click_count))
  }
}
