package cn.njupt.UrlCount

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 同一个学院下访问URL的TOP2
  */
object UrlCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // 将数据切分，元组中放的是（URL， 1）
    val UrlAndOne: RDD[(String, Int)] = sc.textFile("./src/main/resources/itcast.log").map(line => {
      val fields: Array[String] = line.split("\t")
      (fields(1), 1)
    })
    // 将相同URL的次数合并
    val UrlAndMore: RDD[(String, Int)] = UrlAndOne.reduceByKey(_ + _)
    // 对URL进行解析，以学院为key
    val hostAndMore: RDD[(String, (String, Int))] = UrlAndMore.map(line => {
      val url: String = line._1
      val host: String = new URL(url).getHost
      // (php.itcast.cn, 123)
      (host, (url, line._2))
    })

    // 学院的种类
    val institutes: Array[String] = hostAndMore.map(_._1).distinct().collect()

    val result: RDD[(String, (String, Int))] = hostAndMore.partitionBy(new HostPartition(institutes)).mapPartitions(it => {
      it.toList.sortBy(_._2._2).reverse.take(2).iterator
    })

    result.saveAsTextFile("./src/out")

    sc.stop()
  }
}

/**
  * 决定了数据到哪个分区里面
  */
class HostPartition(ins: Array[String]) extends Partitioner {
  private val map: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()
  var count: Int = 0
  for (i <- ins) {
    map(i) = count
    count += 1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = map.getOrElse(key.toString, 0)
}