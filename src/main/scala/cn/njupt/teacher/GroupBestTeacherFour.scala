package cn.njupt.teacher


import java.util

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import scala.collection.JavaConverters._


/**
  * 根据学科分组求每个学科的最受欢迎老师Top3
  * 自定义分区器
  * 改进：减少shuffle次数，排序
  */
object GroupBestTeacherFour {
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

    // 计算有多少个学科
    val subjects = subjectTeacherAndOne.map(_._1._1).distinct().collect()

    val reducedAndPartitioned: RDD[((String, String), Int)] = subjectTeacherAndOne.reduceByKey(new MyPartitioner2(subjects), _ + _)

    val sorted: RDD[((String, String), Int)] = reducedAndPartitioned.mapPartitions(it => {
      val map = new util.TreeMap[Int, ((String, String), Int)]()

      while (it.hasNext) {
        val elem: ((String, String), Int) = it.next()
        if (map.size() < 3) {
          map.put(elem._2, elem)
        } else if (elem._2 > map.firstKey()) {
          map.remove(map.firstKey())
          map.put(elem._2, elem)
        }
      }

      map.values().asScala.toList.reverse.take(3).iterator
    })

    println(sorted.collect().toBuffer)

    sc.stop()
  }
}

class MyPartitioner2(subjects: Array[String]) extends Partitioner {
  private val subjectWithIndex: Map[String, Int] = subjects.zipWithIndex.toMap

  override def numPartitions: Int = subjects.length

  override def getPartition(key: Any): Int = subjectWithIndex(key.asInstanceOf[(String, String)]._1)
}