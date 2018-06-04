package cn.njupt.streamingProj.domain

/**
  * 实战课程点击数实体类
  * @param day_course 对应的就是HBase中的RowKey 20171111_1
  * @param click_count 对应20171111_1的访问总数
  */
case class CourseClickCount(day_course: String, click_count: Long)
