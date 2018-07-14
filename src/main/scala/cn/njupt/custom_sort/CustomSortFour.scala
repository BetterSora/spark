package cn.njupt.custom_sort

import org.apache.spark.{SparkConf, SparkContext}

object CustomSortFour {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortFour").setMaster("local[2]")
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

    implicit val orderedUser: Ordering[User4] = new Ordering[User4] {
      override def compare(x: User4, y: User4): Int = {
        if (x.fv == y.fv) {
          x.age - y.age
        } else {
          y.fv - x.fv
        }
      }
    }
    // 排序(传入了一个排序规则，不会改变数据的格式，只会改变顺序)
    val sorted = userRDD.sortBy(u => User4(u._2, u._3))

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}

case class User4(age: Int, fv: Int)
