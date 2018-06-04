package cn.njupt.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka
  * 参数：hadoop:9092 hello_topic
  */
object KafkaStreamingApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: KafkaDirectWordCount <brokers> <topics>")
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("KafkaStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val topicSet = topics.split(", ").toSet
    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)

    kafkaStream.map(_._2).count().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
