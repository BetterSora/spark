package cn.njupt.streaming

import java.net.InetSocketAddress

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Flume的第二种方式(pull)
  *  agent.sinks = spark
  *  agent.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
  *  agent.sinks.spark.hostname = <hostname of the local machine>
  *  agent.sinks.spark.port = <port to listen on for connection from Spark>
  *  agent.sinks.spark.channel = memoryChannel
  */
object FlumePullWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("FlumePullWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 相当于从服务端拉取数据
    //val address = Seq(new InetSocketAddress("hadoop", 41414)) // 可以从多个flume中拉取数据
    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(ssc, "hadoop", 41414)
    flumeStream.map(x => {
      new String(x.event.getBody.array()).trim
    }).flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
