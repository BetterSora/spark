package cn.njupt.SQL

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * Dataset操作
  * 将DataFrame转成Dataset
  */
object DatasetApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("DatasetApp").master("local[2]").getOrCreate()

    //注意：需要导入隐式转换
    import spark.implicits._

    val path = "file:///Users/rocky/data/sales.csv"

    //spark如何解析csv文件？是否解析头部 是否推断schema信息
    val df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    df.show

    val ds: Dataset[Sales] = df.as[Sales]// 隐式转换就是为了这里
    ds.map(line => line.itemId).show

    spark.stop()
  }

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

}
