package cn.njupt.custom_sort

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 自定义排序
  * 不改变原始数据的格式
  */
object CustomSortTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortTwo").setMaster("local[2]")
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
    val sorted = userRDD.sortBy(u => new User2(u._2, u._3))

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}

/**
  * 实现序列化的原因
  * 规则也要和数据一起shuffle出去，它和数据是一一对应的，规则就是这个对象
  * 虽然他在每个task中被new出来，但是后面要跟着数据一起走网络shuffle到其他分区中
  */
class User2(val age: Int, val fv: Int) extends Ordered[User2] with Serializable {
  override def compare(that: User2): Int = {
    if (this.fv == that.fv) {
      this.age - that.age
    } else {
      that.fv - this.fv
    }
  }
}
