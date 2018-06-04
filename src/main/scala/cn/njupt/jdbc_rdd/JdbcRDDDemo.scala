package cn.njupt.jdbc_rdd

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 利用Spark读取Mysql数据库中数据
  */
object JdbcRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("JdbcRDD").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val connection: () => Connection = () => {
      DriverManager.getConnection("jdbc:mysql://localhost:3306", "root", "root")
    }

    val jdbcRDD : JdbcRDD[(Int, String)] = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM ta where id >= ? AND id <= ?",
      1, 4, 2,
      r => {
        val id = r.getInt(1)
        val code = r.getString(2)
        (id, code)
      }
    )

    println(jdbcRDD.collect().toBuffer)

    sc.stop()
  }
}
