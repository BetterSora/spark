package cn.njupt

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

object RddCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val lines = spark.sparkContext.textFile("hdfs://...")
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map((_, 1))
    val wordCounts = pairs.reduceByKey(_ + _)
    wordCounts.collect().foreach(println(_))

    val sampledPairs = pairs.sample(false, 0.1)
    val sampledWordCounts = sampledPairs.countByKey()
    sampledWordCounts.foreach(println(_))

    spark.stop()
  }

  def solution1(pairs: RDD[(String, Int)]): RDD[(String, Int)] = {
    val prefix: () => Int = () => Random.nextInt(10)
    pairs.map(pair => (prefix() + "-" + pair._1, pair._2)).reduceByKey(_ + _).map(pair => (pair._1.split("-")(1), pair._2)).reduceByKey(_ + _)
  }

  def solution2(spark: SparkSession, pairs: RDD[(String, String)]): Unit = {
    // 首先将数据量比较小的RDD的数据，collect到Driver中来并转成map
    val lines = spark.sparkContext.textFile("hdfs://...")
    val valueToBroadcast = lines.map(line => (line.split(",")(0), line.split(",")(1))).collectAsMap()
    val broadcast = spark.sparkContext.broadcast(valueToBroadcast)
    // inner join
    pairs.filter(pair => broadcast.value.contains(pair._1)).map(pair => broadcast.value(pair._1))
  }

  def solution3(pairs1: RDD[(String, String)], pairs2: RDD[(String, String)]): RDD[(String, (String, String))] = {
    val skewedKey = pairs1.sample(false, 0.1).map(pair => (pair._1, 1)).reduceByKey(_ + _).sortByKey(false).first()._1
    val skewedRdd = pairs1.filter(pair => pair._1 == skewedKey)
    val commonRdd = pairs1.filter(pair => pair._1 != skewedKey)

    // 准备增加随机前缀
    val size = 100
    val prefix: () => Int = () => Random.nextInt(size)
    val prefixSkewedRdd = skewedRdd.map(pair => (prefix() + "-" + pair._1, pair._2))
    // 拷贝size个
    val sizeCopySkewedRdd = pairs2.filter(pair => pair._1 == skewedKey).flatMap(pair => {
      var arr = Array[(String, String)]()
      for (i <- 0 until 100) {
        val newPair = (i + "-" + pair._1, pair._2)
        arr :+= newPair
      }
      arr
    })

    // 附加了随机前缀的独立RDD与另一个膨胀n倍的独立RDD进行join，此时就可以将原先相同的key打散成n份，分散到多个task中去进行join了
    val joinedRdd1 = prefixSkewedRdd.join(sizeCopySkewedRdd).map(pair => {
      val fields = pair._1.split("-")
      (fields(1), pair._2)
    })

    // 另外两个普通的RDD就照常join
    val joinedRdd2 = commonRdd.join(pairs2)
    // 将两次join的结果使用union算子合并起来
    joinedRdd1.union(joinedRdd2)
  }

  def solution4(pairs1: RDD[(String, String)], pairs2: RDD[(String, String)]): RDD[(String, (String, String))] = {
    // 首先，将数据分布相对均匀的RDD膨胀100倍
    val sizeCopyRdd = pairs1.flatMap(pair => {
      val arr = new Array[(String, String)](100)

      for (i <- 0 until 100) {
        arr(i) = (i + "-" + pair._1, pair._2)
      }

      arr
    })

    // 其次，将另一个有数据倾斜key的RDD，每条数据都打上100以内的随机前缀
    val prefix: () => Int = () => Random.nextInt(100)
    val skewedRdd = pairs2.map(pair => (prefix() + "-" + pair._1, pair._2))

    // 将两个处理后的RDD进行join即可
    sizeCopyRdd.join(skewedRdd)
  }
}
