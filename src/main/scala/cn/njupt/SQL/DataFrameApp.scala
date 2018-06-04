package cn.njupt.SQL

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * DataFrame API操作
  */
object DataFrameApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()
    import spark.implicits._

    // 将json文件加载成一个dataframe
    val peopleDF = spark.read.format("json").load("./src/main/resources/people.json")

    // 输出dataframe对应的schema信息
    peopleDF.printSchema()

    // 输出数据集的前20条记录
    peopleDF.show()

    // 查询某列所有数据: select name from table
    peopleDF.select("name").show()

    // 查询某几列的数据，并对列进行计算: select name, age+10 as age2 from table
    //peopleDF.select(peopleDF.col("name"), (peopleDF.col("age") + 10).as("age2")).show()
    peopleDF.select($"name", ($"age" + 10) as "age2").show()

    // 按照某一列的值进行过滤: select * from table where age > 19
    val rs: Dataset[Row] = peopleDF.filter(peopleDF.col("age") > 10)
    rs.show()

    // 根据某一列进行分组，然后在进行聚合操作: select age,count(1) from table group by age
    peopleDF.groupBy("age").count().show()

    spark.stop()
  }
}
