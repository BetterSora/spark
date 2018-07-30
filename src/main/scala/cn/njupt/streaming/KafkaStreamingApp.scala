package cn.njupt.streaming

import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka，使用直连方式并用checkpoint管理偏移量
  * 参数：hadoop:9092 hello_topic
  * To recover from driver failures, you have to enable checkPointing in the `StreamingContext`.
  *
  * 说一下这个方法的缺点：
  * 这个方法不适合总是需要迭代升级的应用，因为这个方法会在你建立检查点时将你的jar包信息以序列化的方式存在此目录中，
  * 如果你的作业挂掉重新启动时，这时候是没有问题的，因为什么都没有改变。
  * 但是在你的应用迭代升级时你的代码发生了变化，这是程序会发现其中的变化，你迭代升级后的版本将无法运行，就算是启动成功了，
  * 运行的也还是迭代升级之前的代码。所还是以失败而告终！！
  * 在Spark官方文档中给出了两个解决办法
  * 第一个：老的作业不停机，新作业个老作业同时运行一段时间，这样是不安全的！！！
  * 会导致数据重复消费，也有可能会发生数据丢失等问题
  * 第二个：就是我要讲的自己维护消息偏移量
  */
object KafkaStreamingApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def createStreaming(checkPointDir: String): StreamingContext = {
    val conf = new SparkConf()
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint(checkPointDir)

    val brokerList = "hadoop:9092"
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> brokerList)
    val topics = Set("testTopic")
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    // 处理业务逻辑
    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        for (tuple <- it) {
          println(tuple._1, tuple._2)
        }
      })
    })

    ssc
  }

  def main(args: Array[String]): Unit = {
    // checkpoint存放数据的地址
    val checkPointDir = "."
    // StreamingContext创建函数
    val creatingFunc: () => StreamingContext = () => {
      createStreaming(checkPointDir)
    }

    val ssc = StreamingContext.getOrCreate(checkPointDir, creatingFunc)
    ssc.start()
    ssc.awaitTermination()
  }
}
