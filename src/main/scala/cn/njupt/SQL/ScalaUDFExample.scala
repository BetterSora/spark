package cn.njupt.SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * UDF对表中的单行进行转换，以便为每行生成单个对应的输出值
  * 另一种方式:cn.njupt.ip.IpLocationSQLThree
  */
object ScalaUDFExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala UDF Example").setMaster("local[2]")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    val df = spark.read.json("./src/main/resources/temperatures.json")
    df.createOrReplaceTempView("citytemps")
    spark.udf.register("CTOF", (t: Double) => (t * 9.0 / 5.0) + 32.0)

    spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()

    spark.stop()
  }
}
