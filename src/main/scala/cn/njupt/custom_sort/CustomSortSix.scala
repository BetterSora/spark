package cn.njupt.custom_sort

import org.apache.spark.{SparkConf, SparkContext}

object CustomSortSix {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortSix").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines = sc.parallelize(users)

    val userRDD = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt

      (name, age, fv)
    })

    // 充分利用元组的比较规则，元组的比较规则：先比第一，相等再比第二个
    // Ordering[(Int, Int)]最终比较的规则格式
    // on[(String, Int, Int)]未比较之前的数据格式
    // (t =>(-t._3, t._2))怎样将规则转换成想要比较的格式
    implicit val rules: Ordering[(String, Int, Int)] = Ordering[(Int, Int)].on[(String, Int, Int)](t => (-t._3, t._2))
    // 排序(传入了一个排序规则，不会改变数据的格式，只会改变顺序)
    val sorted = userRDD.sortBy(u => u)

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}
