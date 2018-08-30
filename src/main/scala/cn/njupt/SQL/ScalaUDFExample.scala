package cn.njupt.SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * UDF对表中的单行进行转换，以便为每行生成单个对应的输出值
  * 另一种方式:cn.njupt.ip.IpLocationSQLThree
  */
object ScalaUDFExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala UDF Example").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val df = spark.read.json("./src/main/resources/temperatures.json")
    df.createOrReplaceTempView("citytemps")
    val CTOF: UserDefinedFunction = spark.udf.register("CTOF", (t: Double) => (t * 9.0 / 5.0) + 32.0)

    df.select($"city", CTOF($"avgLow") as "avgLowF", CTOF($"avgHigh") as "avgHigh").show
    spark.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show

    spark.stop()
  }
}
