package cn.njupt.Accumulator

import org.apache.spark.{SparkConf, SparkContext}

object AccumulatorApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[1]").setAppName("AccumulatorApp")
    val sc = new SparkContext(conf)

    var count = 0
    val accumulator = sc.longAccumulator("Example Accumulator")
    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
    val result = rdd.map(num => { count += num; accumulator.add(1) })
    result.collect()
    result.foreach(println(_)) // 其实累加器为10，因为要重新计算一遍

    println(count)
    println(accumulator.value)

    sc.stop()
  }
}
