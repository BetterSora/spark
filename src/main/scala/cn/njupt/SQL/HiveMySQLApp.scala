package cn.njupt.SQL

import org.apache.spark.sql.SparkSession

/**
 * 使用外部数据源综合查询Hive和MySQL的表数据
 */
object HiveMySQLApp {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("HiveMySQLApp")
      .master("local[2]").getOrCreate()

    // 加载Hive表数据
    val hiveDF = spark.table("emp")

    // 加载MySQL表数据，虽然这里不会真正的读数据，但是这里会连接数据库获取schema信息，所以这里连接要是通的
    val mysqlDF = spark.read.format("jdbc").option("url", "jdbc:mysql://localhost:3306")
      .option("dbtable", "spark.DEPT").option("user", "root")
      .option("password", "root").option("driver", "com.mysql.jdbc.Driver").load()

    // Inner JOIN   后面是join的条件
    val resultDF = hiveDF.join(mysqlDF, hiveDF.col("deptno") === mysqlDF.col("DEPTNO"))
    resultDF.show

    resultDF.select(hiveDF.col("empno"),hiveDF.col("ename"),
      mysqlDF.col("deptno"), mysqlDF.col("dname")).show

    spark.stop()
  }

}
