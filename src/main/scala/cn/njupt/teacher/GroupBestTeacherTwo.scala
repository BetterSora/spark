package cn.njupt.teacher

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 根据学科分组求每个学科的最受欢迎老师Top3
  * 用RDD的排序方法
  */
object GroupBestTeacherTwo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupBestTeacherTwo").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("F:/java-proj/ideaProj/Spark/src/main/resources/teacher.log", 3)
    // http://bigdata.edu360.cn/laozhang
    val subjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val index = line.lastIndexOf("/")
      val teacher = line.substring(index + 1)
      val subject = line.split("/")(2).split("[.]")(0)

      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(_ + _)

    val subjects = Array("javaee", "bigdata", "php")
    val isASC = false

    // Scala的集合排序是在内存中进行的，但是内存有可能不够用
    // 可以调用RDD的sortBy方法，内存+磁盘进行排序
    for (sb <- subjects) {
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)
      val sorted: RDD[((String, String), Int)] = filtered.sortBy(_._2, isASC)

      println(sorted.take(3).toBuffer)
    }

    sc.stop()
  }
}
