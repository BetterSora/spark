package cn.njupt.SQLProj

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

/**
  * 日期时间解析工具类
  * 10/Nov/2016:00:01:02 +0800
  * 注意：SimpleDateFormat线程不安全
  */
object DateUtils {
  // 输入文件的日期格式
  val OLD_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss z", Locale.ENGLISH)
  // 目标日期格式
  val NEW_TIME_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time: String): String = {
    var seconds = 0L

    try {
      seconds = OLD_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime
    } catch {
      case e: Exception => e.printStackTrace()
    }

    NEW_TIME_FORMAT.format(new Date(seconds))
  }
}
