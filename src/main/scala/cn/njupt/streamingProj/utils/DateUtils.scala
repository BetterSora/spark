package cn.njupt.streamingProj.utils

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 时间转化工具类
  */
object DateUtils {
  val OLD_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
  val NEW_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

  def parse(time: String): String = {
    val old = OLD_TIME_FORMAT.parse(time)
    NEW_TIME_FORMAT.format(old)
  }

  def main(args: Array[String]): Unit = {
    println(parse("2018-11-11 11:11:11"))
  }
}
