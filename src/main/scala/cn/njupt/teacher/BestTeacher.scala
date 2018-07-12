package cn.njupt.teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 求最受欢迎老师Top3
  */
object BestTeacher {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BestTeacher").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("F:/java-proj/ideaProj/Spark/src/main/resources/teacher.log", 1)

    val teachersAndOne: RDD[(String, Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      (line.substring(index + 1), 1)
    })
    val isASC = false
    val reduced: RDD[(String, Int)] = teachersAndOne.reduceByKey(_ + _)
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, isASC)
    println(sorted.take(3).toBuffer)

    sc.stop()
  }
}
