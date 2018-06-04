package cn.njupt.custom_sort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 定义隐式转换上下文
  */
object OrderContext {
  implicit val girlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) {
        1
      } else if (x.faceValue == y.faceValue) {
        if (x.age > y.age) -1 else 1
      } else {
        -1
      }
    }
  }
}

/**
  * Spark自定义排序
  */
object CustomSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val needToSortRDD: RDD[(String, Int, Int, Int)] = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)))
    import OrderContext.girlOrdering
    val sortedRDD: RDD[(String, Int, Int, Int)] = needToSortRDD.sortBy(value => Girl(value._2, value._3), false)
    println(sortedRDD.collect().toBuffer)

    sc.stop()
  }
}

/**
  * rdd之间排序需要走网络
  */
case class Girl(faceValue: Int, age: Int) extends Serializable