package cn.njupt.SQL

import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * SparkSession的使用
  */
object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()

    val people: DataFrame = spark.read.json("./src/main/resources/people.json")
    people.show()

    spark.stop()
  }
}
