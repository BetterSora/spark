package cn.njupt.teacher

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 根据学科分组求每个学科的最受欢迎老师Top3
  * 用Scala的排序方法
  */
object GroupBestTeacherOne {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BestTeacher").setMaster("local[2]")
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
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)

    // 经过分组后，一个分区内可能有多个学科的数据，一个学科就是一个迭代器
    // 将每一个组拿出来进行操作
    // 为什么可以调用scala的sortBy方法呢？因为一个学科的数据已经在一台机器上的一个scala集合里面了
    val sorted: RDD[List[((String, String), Int)]] = grouped.map(tuple => {
      val it = tuple._2
      it.toList.sortBy(_._2).reverse.take(3)
    })

    println(sorted.collect().toBuffer)
    sc.stop()
  }
}
