package cn.njupt.SQL

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * HiveContext的使用
  * 需要通过--jars把mysql的驱动传递到classpath里面
  */
object HiveContextApp {
  def main(args: Array[String]): Unit = {
    // 创建相应的context
    // 在测试或者生产中，AppName和Master我们是通过脚本进行指定
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    // HiveContext是SQLContext的子类，与hive有部分耦合，支持hql，使用了Hive的MetaStore
    val hiveContext = new HiveContext(sc)

    // 相关处理
    hiveContext.table("emp").show()

    // 关闭资源
    sc.stop()
  }
}
