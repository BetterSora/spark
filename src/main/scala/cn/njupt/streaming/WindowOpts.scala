package cn.njupt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming window函数使用
  */
object WindowOpts {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("WindowOpts").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop", 6789)
    val wordAndOne: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1))
    // 每隔10S统计前15S的数据
    val result = wordAndOne.reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(15), Seconds(10))
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
