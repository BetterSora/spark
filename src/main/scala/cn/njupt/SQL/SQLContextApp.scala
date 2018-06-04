package cn.njupt.SQL

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * SQLContext的使用
  */
object SQLContextApp {
  def main(args: Array[String]): Unit = {
    val path = args(0)

    // 创建相应的context
    // 在测试或者生产中，AppName和Master我们是通过脚本进行指定
    val conf = new SparkConf().setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // SQLContext与hive解耦，不支持hql查询
    val sqlContext = new SQLContext(sc)

    // 相关处理(json)
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    // 关闭资源
    sc.stop()
  }
}
