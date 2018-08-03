package cn.njupt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * select avg(age) from temp group by dpt;
  */
object RddAndDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("RddAndDataFrame").master("local[2]").getOrCreate()
    val data = Array(("a", 20),("b", 33),("c", 24),("a", 12),("b", 56),("c", 23),("d", 56))

    val dptAndAge: RDD[(String, Int)] = spark.sparkContext.parallelize(data)
    val dptAndAgeAndOne: RDD[(String, (Int, Int))] = dptAndAge.map {case(dpt, age) => dpt -> (age, 1)}
    val reduced: RDD[(String, (Int, Int))] = dptAndAgeAndOne.reduceByKey {case ((a1, c1), (a2, c2)) => (a1 + a2, c1 + c2)}
    // 在map端会排序，但是拉到reduce后不会再合并排序了
    //reduced.glom().collect().foreach(a => println(a.toBuffer))
    val result: RDD[(String, Int)] = reduced.map {case(dpt, (age, c)) => dpt -> age / c}
    println(result.collect().toBuffer)

    spark.stop()
  }
}
