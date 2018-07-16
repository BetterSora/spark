package cn.njupt.ip

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * jon的代价太昂贵，而且非常慢，解决思路是将表缓存起来（广播变量）
  */
object IpLocationSQLThree {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("IpLocationSQLTwo").master("local[2]").getOrCreate()

    // 取到HDFS中的ip规则
    import spark.implicits._

    val rulesLines:Dataset[String] = spark.read.textFile(args(0))
    // 整理ip规则数据()
    val rulesDataset = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    // 收集ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = rulesDataset.collect()
    // 广播(必须使用SparkContext)
    // 将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)

    // 创建RDD，读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))

    // 整理数据
    val ipDataFrame: DataFrame = accessLines.map(log => {
      // 将log日志的每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      // 将ip转换成十进制
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ipDataFrame.createTempView("v_log")

    import org.apache.spark.sql.functions._
    val ip2Province: UserDefinedFunction = udf((ipNum: Long) => {
      // 查找ip规则（事先已经广播了，已经在Executor中了）
      // 函数的逻辑是在Executor中执行的，怎样获取ip规则的对应的数据呢？
      // 使用广播变量的引用，就可以获得
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      // 根据IP地址对应的十进制查找省份名称
      val index = MyUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if (index != -1) {
        province = ipRulesInExecutor(index)._3
      }
      province
    })

    val r = ipDataFrame.select(ip2Province($"ip_num") as "province").groupBy($"province").agg(count($"province") as "counts").orderBy($"counts" desc)
    r.show()

    spark.stop()
  }
}
