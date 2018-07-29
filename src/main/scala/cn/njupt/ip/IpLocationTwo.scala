package cn.njupt.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocationTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocationTwo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // 取到HDFS中的ip规则
    val rulesLines:RDD[String] = sc.textFile(args(0))
    // 整理ip规则数据
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    // 将分散在多个Executor中的部分IP规则收集到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()

    // 将Driver端的数据广播到Executor
    // 广播变量的引用（还在Driver端）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rulesInDriver)

    // 创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile(args(1))

    // 整理数据
    val provinceAndOne: RDD[(String, Int)] = accessLines.map(log => {
      // 将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      // 将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      // 进行二分法查找，通过Driver端的引用或取到Executor中的广播变量
      //（该函数中的代码是在Executor中别调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播的规则了）
      // Driver端广播变量的引用是怎样跑到Executor中的呢？
      // Task是在Driver端生成的，广播变量的引用是伴随着Task被发送到Executor中的
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 查找
      var province = "未知"
      val index = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    })

    // 聚合
    //val sum = (x: Int, y: Int) => x + y
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey(_+_)

    // 将结果打印
    //val r = reduced.collect()
    //println(r.toBuffer)

    // 将结果写入数据库
    reduced.foreachPartition(it => MyUtils.data2MySQL(it))

    sc.stop()
  }
}
