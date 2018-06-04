package cn.njupt.SQLProj

import com.ggstar.util.ip.IpHelper

/**
  * Ip解析工具类
  */
object IpUtils {
  def getProvince(ip: String): String = {
    IpHelper.findRegionByIp(ip)
  }

  def main(args: Array[String]): Unit = {
    println(getProvince("223.65.190.22"))
  }
}
