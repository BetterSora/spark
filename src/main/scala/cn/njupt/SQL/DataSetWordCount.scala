package cn.njupt.SQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Spark 2.X API进行WordCount
  * DSL方式(domain-specific languages )
  */
object DataSetWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile(args(0))
    // 整理数据(切分压平)
    // 导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    // 导入聚合函数
    import org.apache.spark.sql.functions._
    val df: DataFrame = words.groupBy($"value" as "word").agg(count($"value") as "count")

    df.show()

    spark.stop()
  }
}
