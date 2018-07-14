package cn.njupt.custom_sort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序
  */
object CustomSortOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortOne").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 排序规则：首先按照颜值的降序，如果颜值相等，再按照年龄的升序
    val users= Array("laoduan 30 99", "laozhao 29 9999", "laozhang 28 98", "laoyang 28 99")
    val lines = sc.parallelize(users)

    val userRDD = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val fv = fields(2).toInt

      new User1(name, age, fv)
    })

    val sorted = userRDD.sortBy(u => u)

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}

class User1(val name: String, val age: Int, val fv: Int) extends Ordered[User1] with Serializable {
  override def toString: String = s"name: $name age: $age fv: $fv"

  override def compare(that: User1): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      that.fv - this.fv
    }
  }
}