package cn.njupt.teacher

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
  * 根据学科分组求每个学科的最受欢迎老师Top3
  * 自定义分区器
  */
object GroupBestTeacherThree {
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

    val subjects = reduced.map(_._1._1).distinct().collect()
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(new MyPartitioner1(subjects))
    val sorted = partitioned.mapPartitions(it => it.toList.sortBy(_._2).reverse.take(3).iterator)

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}

class MyPartitioner1(subjects: Array[String]) extends Partitioner {
  private val subjectWithIndex: Map[String, Int] = subjects.zipWithIndex.toMap

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = subjectWithIndex(key.asInstanceOf[(String, String)]._1)
}
