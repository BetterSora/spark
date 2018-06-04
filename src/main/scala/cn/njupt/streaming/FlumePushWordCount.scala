package cn.njupt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Flume的第一种方式(push)
  */
object FlumePushWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("FlumePushWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 相当于启动了一个服务端等待数据发来，这种方式不好，因为以后再分布式集群中，这种方式只能有一台机器接收数据
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createStream(ssc, "0.0.0.0", 41414)
    flumeStream.map(x => {
      new String(x.event.getBody.array()).trim
    }).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
