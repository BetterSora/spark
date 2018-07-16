package cn.njupt.SQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Spark整合Hive
  */
object HiveOnSpark {
  def main(args: Array[String]): Unit = {
    System.setProperty("DHADOOP_USER_NAME", "qinzhen")

    // 如果想让hive运行在spark上，一定要开启spark对hive的支持
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport() // 启用spark对hive的支持(可以兼容hive的语法了)
      .getOrCreate()

    // 想要使用hive的元数据库，必须指定hive元数据的位置，添加一个hive-site.xml到当前程序的classpath下即可
    import spark.implicits._
    val rdd: RDD[String] = spark.sparkContext.parallelize(List("zhangsan,18","lisi,20","laoduan,18"))
    val df: DataFrame = rdd.map(line => (line.split(",")(0), line.split(",")(1).toInt)).toDF("name", "age")
    df.createTempView("person")

    val sql1: DataFrame = spark.sql("select * from app.dim_user_active_day limit 10")
    sql1.show
    val sql2: DataFrame = spark.sql("select * from person limit 10")
    sql2.show

    spark.stop()
  }
}