package cn.njupt.SQLProj

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 根据统计要求对数据进行二次清洗
  */
object SparkStatCleanJob {
  val struct = StructType(
    Array(
      StructField("url", StringType),
      StructField("cmsType", StringType),
      StructField("cmsId", LongType),
      StructField("traffic", LongType),
      StructField("ip", StringType),
      StructField("province", StringType),
      StructField("time", StringType),
      StructField("day", StringType)))

  /**
    * 对每行数据进行清洗，并转化为Row输出
    */
  def clean(line: String): Row = {
    // 2016-11-10 00:01:28	http://www.imooc.com/video/2752	54	61.183.118.245
    // fields: url cmsType cmsId traffic ip province time day
    try {
      val fields = line.split("\t")
      val url = fields(1)

      val domain = "http://www.imooc.com/"
      val cmsTypeID = url.substring(url.indexOf(domain) + domain.length).split("/")
      val cmsType = cmsTypeID(0)
      val cmsId = cmsTypeID(1).toLong

      val traffic = fields(2).toLong
      val ip = fields(3)
      val province = IpUtils.getProvince(ip)
      val time = fields(0)
      val day = time.split(" ")(0).replaceAll("-", "")

      Row(url, cmsType, cmsId, traffic, ip, province, time, day)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Row(0)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkStatFormatJob").master("local[2]").getOrCreate()

    val log: RDD[String] = spark.sparkContext.textFile("./src/main/resources/one_step_access.log")

    // RDD => DataFrame
    val df: DataFrame = spark.createDataFrame(log.filter(line => line.split("\t")(1).matches("http://www.imooc.com/(video|article)/\\d+")).map(line => clean(line)), struct)
    df.show(false)

    // 将DF保存为parquet格式存储
    df.write.format("parquet").mode("overwrite").partitionBy("day").save("./output2/")

    spark.stop()
  }
}
