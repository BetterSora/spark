package cn.njupt.ip

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object IpLocationSQLOne {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("IpLocationSQLOne").master("local[2]").getOrCreate()
    import spark.implicits._
    // 读取规则
    val rules: Dataset[String] = spark.read.textFile(args(0))
    val ruleDataFrame: DataFrame = rules.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")

    // 读取访问日志
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

    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")

    val r = spark.sql("SELECT province, count(*) counts FROM v_ips JOIN v_rules ON (ip_num >= snum AND ip_num <= enum) GROUP BY province ORDER BY counts DESC")
    r.explain()
    r.show()

    spark.stop()
  }
}
