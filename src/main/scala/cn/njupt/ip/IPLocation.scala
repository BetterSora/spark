package cn.njupt.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据ip表查询网关日志中ip所属的省
  */
object IPLocation {
  /**
    * 将ip转成long类型数
    */
  def ip2Long(ip: String): Long = {
    val fragments: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找数据
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
    var low: Int = 0
    var high: Int = lines.length - 1
    while (low <= high) {
      val middle: Int = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IPLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 读取ip对照表
    val ipRulesRdd: RDD[(String, String, String)] = sc.textFile("./src/main/resources/ip.txt").map(line => {
      val fields: Array[String] = line.split("\\|")
      val start: String = fields(2)
      val end: String = fields(3)
      val province: String = fields(6)
      // (16777472, 16778239, 福建)
      (start, end, province)
    })

    // 将ip对照表进行广播，给所有Task使用
    val ipRulesBroadcast: Broadcast[Array[(String, String, String)]] = sc.broadcast(ipRulesRdd.collect())

    // 加载待处理的数据 (125.213.100.123, 117.101.215.133...)
    val ipsRDD: RDD[String] = sc.textFile("./src/main/resources/access.log").map(line => line.split("\\|")(1))

    // 处理数据并获得结果
    val result: RDD[(String, String, String)] = ipsRDD.map(ip => {
      val ipNum: Long = ip2Long(ip)
      val index: Int = binarySearch(ipRulesBroadcast.value, ipNum)
      val info: (String, String, String) = ipRulesBroadcast.value(index)
      info
    })

    println(result.collect().toBuffer)

    sc.stop()
  }
}
