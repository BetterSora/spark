package cn.njupt.SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameRDDApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameRDDApp").master("local[2]").getOrCreate()

    //inferReflection(spark)
    //program(spark)
    // 第三种方式，最简单！
    import spark.implicits._
    val rdd = spark.sparkContext.textFile("./src/main/resources/infos.txt")
    val df = rdd.map(_.split(",")).map(fields => (fields(0).toInt, fields(1), fields(2).toInt)).toDF("id", "name", "age")
    df.show()
  }

  def program(spark: SparkSession): Unit = {
    // 通过编程的方式 RDD => DataFrame 事先不知道schema信息
    val rdd = spark.sparkContext.textFile("./src/main/resources/infos.txt")
    val infoRDD = rdd.map(_.split(",")).map(fields => Row(fields(0).toInt, fields(1), fields(2).toInt))

    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true))) // 第三个参数为是否允许null值

    val infoDF = spark.createDataFrame(infoRDD, structType)
    infoDF.printSchema()
    infoDF.show()

    spark.stop()
  }

  def inferReflection(spark: SparkSession): Unit = {
    // 通过反射的方式 RDD => DataFrame
    val infoRDD = spark.sparkContext.textFile("./src/main/resources/infos.txt")

    // 导入隐式转换，为了将RDD转成DataFrame
    import spark.implicits._
    infoRDD.map(_.split(",")).map(fields => info(fields(0).toInt, fields(1), fields(2).toInt)).toDS()
    val infoDF = infoRDD.map(_.split(",")).map(fields => info(fields(0).toInt, fields(1), fields(2).toInt)).toDF()
    infoDF.show()

    infoDF.filter(infoDF.col("age") > 30).show()

    // registerTempTable()已经过时了
    infoDF.createOrReplaceTempView("info")
    spark.sql("select * from info where age > 30").show()

    spark.stop()
  }

  case class info(id: Int, name: String, age: Int)
}
