package cn.njupt.wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark的WordCount
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val config = new SparkConf().setAppName("WordCount").setMaster("local[2]")
    val sc = new SparkContext(config)

    // textFile会产生两个RDD：HadoopRDD -> MapPartitionsRDD
    sc.textFile(args(0))
      // 产生一个RDD：MapPartitionsRDD
      .flatMap(_.split(" "))
      // 产生一个RDD：MapPartitionsRDD
      .map((_, 1))
      // 产生一个RDD：ShuffledRDD
      .reduceByKey(_ + _)
      // 产生一个RDD：MapPartitionsRDD
      .saveAsTextFile(args(1))
    // 必须要关闭，否则出错
    sc.stop()
  }
}
