package cn.njupt.SQL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Spark 2.X API进行WordCount
  * SQL方式
  */
object SQLWordCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLWordCount").master("local[*]").getOrCreate()

    // is not a Parquet file 默认是读取parquet格式的文件
    //val lines = spark.read.load(args(0))
    val lines: Dataset[String] = spark.read.textFile(args(0))
    // 整理数据(切分压平)
    // 导入隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
    //words.show()
    //words.map((_, 1)).toDF("word", "count").show()
    // 注册视图
    words.createTempView("t_word")

    // 执行SQL（Transformation，lazy）
    val result: DataFrame = spark.sql("SELECT value, COUNT(*) AS count FROM t_word GROUP BY value ORDER BY count DESC")
    // 执行Action
    result.show(false)

    spark.stop()
  }
}
