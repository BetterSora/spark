package cn.njupt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming处理文件系统(local/hdfs)数据
  */
object FileWordCount {
  // 更改日志输出级别
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("FileWordCount").setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 设置checkpoint地址
    ssc.checkpoint(".")

    // 这路径如果hdfs的路径 你直接hadoop fs  -put 到你的监测路径就可以
    // 如果是本地目录用file:///home/data 你不能移动文件到这个目录，必须用流的形式写入到这个目录形成文件才能被监测到。
    val line: DStream[String] = ssc.textFileStream("F://Data")
    // 虽然数据可靠性得到保障了，但是要谨慎的设置刷新间隔，这可能会影响吞吐量，因为每隔固定时间都要向HDFS上写入checkpoint数据
    // spark streaming官方推荐checkpoint定时持久的刷新间隔一般为批处理间隔的5到10倍是比较好的一个方式。
    line.checkpoint(Seconds(50))
    val result: DStream[(String, Int)] = line.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}