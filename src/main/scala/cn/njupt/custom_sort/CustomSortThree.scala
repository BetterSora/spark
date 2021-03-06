package cn.njupt.custom_sort

import org.apache.spark.{SparkConf, SparkContext}

object CustomSortThree {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortThree").setMaster("local[2]")
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

    // 排序(传入了一个排序规则，不会改变数据的格式，只会改变顺序)
    val sorted = userRDD.sortBy(u => User3(u._2, u._3))

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}

/**
  * 这里不需要实现序列化(case class本身实现了序列化)
  * 但是这么做写死了规则，以后我想要千变万化的规则就很麻烦
  */
case class User3(age: Int, fv: Int) extends Ordered[User3] /*with Serializable*/ {
  override def compare(that: User3): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      that.fv - this.fv
    }
  }
}
