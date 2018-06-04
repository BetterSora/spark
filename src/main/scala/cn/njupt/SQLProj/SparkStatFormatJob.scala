package cn.njupt.SQLProj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 第一步清洗：抽取出我们需要的指定列的数据
  */
object SparkStatFormatJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val log: RDD[String] = spark.sparkContext.textFile("./src/main/resources/10000_access.log")

    val cleanLog: RDD[String] = log.map((line: String) => {
      val fields = line.split(" ")
      val ip = fields(0)
      val time = fields(3) + " " + fields(4)
      val url = fields(11).replaceAll("\"", "")
      val traffic = fields(9)

      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    })

    cleanLog.repartition(1).saveAsTextFile("./output/")

    spark.stop()
  }
}
