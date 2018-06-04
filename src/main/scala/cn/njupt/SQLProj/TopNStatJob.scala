package cn.njupt.SQLProj

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ArrayBuffer

/**
  * 统计最受欢迎的TopN课程
  */
object TopNStatJob {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  case class DayVideoAccessStat(day: String, cmiId: Long, times: Long)

  case class DayProvinceVideoAccessStat(day: String, cmsId: Long, province: String, times: Long, timesRank: Int)

  /**
    * 访问次数最多的TopN课程
    * fields: url cmsType cmsId traffic ip province time day
    */
  def videoAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._

    // 使用DF API进行统计
    val videoTopNDF: DataFrame = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy($"day", $"cmsId").agg(count("cmsId").as("times")).orderBy($"times")

    // 将统计结果写入MySQL
    try {
      videoTopNDF.foreachPartition((partitionOfRecords: Iterator[Row]) => {
        val buffer = new ArrayBuffer[DayVideoAccessStat]()

        // 将每个分区中数据缓存起来，一起进行处理
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val times = info.getAs[Long]("times")
          // 不建议在此处进行数据库的数据插入，而是一个分区中的数插入一次
          buffer.append(DayVideoAccessStat(day, cmsId, times))
        })

        // 将数据插入数据库
        StatDAO.insertDayVideoAccessTopN(buffer)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照省市进行统计TopN课程
    */
  def provinceAccessTopNStat(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._

    // 使用DF API进行统计
    val provinceTopNDF: DataFrame = accessDF.filter($"day" === day && $"cmsType" === "video")
      .groupBy($"day", $"province", $"cmsId").agg(count("cmsId").as("times")).orderBy($"times")
    provinceTopNDF.show()

    // Window函数在Spark SQL的使用
    // row_number函数作用于一个分区，并为该分区中的每条记录生成一个序列号
    // PARTITION BY ：指定窗口函数分组的Key
    // orderBy: 组内排序
    val top3DF: DataFrame = provinceTopNDF.select($"day", $"province", $"cmsId", $"times",
      row_number().over(Window.partitionBy("province").orderBy($"times".desc)).as("times_rank"))
      .filter($"times_rank" <= 3) // Top3
    top3DF.show()


    // 将统计结果写入MySQL
    try {
      top3DF.foreachPartition((partitionOfRecords: Iterator[Row]) => {
        val buffer = new ArrayBuffer[DayProvinceVideoAccessStat]()

        // 将每个分区中数据缓存起来，一起进行处理 row类型用getAs
        partitionOfRecords.foreach(info => {
          val day = info.getAs[String]("day")
          val cmsId = info.getAs[Long]("cmsId")
          val province = info.getAs[String]("province")
          val times = info.getAs[Long]("times")
          val timesRank = info.getAs[Int]("times_rank")
          // 不建议在此处进行数据库的数据插入，而是一个分区中的数插入一次
          buffer.append(DayProvinceVideoAccessStat(day, cmsId, province, times, timesRank))
        })

        // 将数据插入数据库
        StatDAO.insertDayProvinceVideoAccessTopN(buffer)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
    * 按照流量进行统计的TopN课程
    */
  def videoTrafficsTopNSta(spark: SparkSession, accessDF: DataFrame, day: String): Unit = {
    import spark.implicits._

    val trafficTopNDF: DataFrame = accessDF.filter($"day" === day && $"cmsType" === "video").groupBy($"day", $"cmsId")
      .agg(sum($"traffic").as("traffics")).orderBy($"traffics".desc)

    // 第二种写入数据库的方式
    trafficTopNDF.write.mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop:3306/SQLProject")
      .option("dbtable", "day_video_traffics_topn_stat")
      .option("user", "root")
      .option("password", "root")
      .save()
  }

  def main(args: Array[String]): Unit = {
    // 关闭schema类型的自动推断，如果不关闭则day会被解析成Integer类型
    val spark = SparkSession.builder().appName("TopNStatJob").master("local[2]")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false").getOrCreate()

    // 读取数据
    val accessDF: DataFrame = spark.read.format("parquet").load("./output2")

    accessDF.cache()

    val day = "20161110"

    StatDAO.deleteData(day)

    // 访问次数最多的TopN课程
    videoAccessTopNStat(spark, accessDF, day)

    // 按照地市进行统计TopN课程
    provinceAccessTopNStat(spark, accessDF, day)

    // 按照流量进行统计的TopN课程
    videoTrafficsTopNSta(spark, accessDF, day)

    accessDF.unpersist()

    spark.stop()
  }
}
