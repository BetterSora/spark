package cn.njupt.streaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming对接Kafka的方式二
  */
object KafkaDirectWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("KafkaDirectWordCount").setMaster("local[4]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    // 指定组名
    val group = "sky"
    // 指定topic名字
    val topic = "testTopic"
    // 指定kafka的broker地址(SparkStreaming的Task直连到kafka的分区上，用更加底层的API消费，效率更高)
    val brokerList = "shizhan:9092"
    // 指定zk的地址，后期更新消费的偏移量时使用(以后可以使用Redis、MySQL来记录偏移量)
    val zkQuorum = "shizhan:2181"
    // 创建stream时使用的topic名字集合，SparkStreaming可同时消费多个topic
    val topicSet = Set(topic)
    // 创建一个ZKGroupTopicDirs对象,其实是指定往zk中写入数据的目录，用于保存偏移量
    val topicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(group, topic)
    // 获取zookeeper中的路径"/consumers/sky/offsets/testTopic/"
    val offsetDir = topicDirs.consumerOffsetDir
    // 准备kafka的参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> group,
      // 如果没有已经读取的记录就从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )

    // zookeeper的host和ip，创建一个client，用于更新偏移量量的
    // 是zookeeper的客户端，可以从zk中读取偏移量数据，并更新偏移量
    // 为什么用这个包的zkClient不用new ZooKeeper，因为更新偏移量指定了这个包的客户端
    val zkClient = new ZkClient(zkQuorum)
    // 查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    // /consumers/sky/offsets/testTopic/0/10001"
    // /consumers/sky/offsets/testTopic/1/30001"
    // /consumers/sky/offsets/testTopic/2/10001"
    // offsetDir  -> /consumers/sky/offsets/testTopic/
    val children: Int = zkClient.countChildren(offsetDir)
    var kafkaStream: InputDStream[(String, String)] = null
    // 如果zookeeper中有保存offset，我们会利用这个offset作为kafkaStream的起始位置
    var fromOffsets: Map[TopicAndPartition, Long] = Map()

    // 如果保存过offset
    if (children > 0) {
      // 遍历每个这个topic的每个分区获取offset
      for (i <- 0 until children) {
        // 读取分区offset
        val partitionOffset : String = zkClient.readData[String](s"$offsetDir/$i")
        val tp = TopicAndPartition(topic, i)
        // 将不同partition对应的offset增加到fromOffsets中
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      // Key: kafka的key(null)   values: "hello tom hello jerry"
      // 这个会将kafka的消息进行transform，最终kafka的数据都会变成(kafka的key, message)这样的tuple
      val messageHandler: MessageAndMetadata[String, String] => (String, String) = mmd => (mmd.key(), mmd.message())

      // 通过KafkaUtils创建直连的DStream（fromOffsets参数的作用是:按照前面计算好了的偏移量继续消费数据）
      // [String, String, StringDecoder, StringDecoder, (String, String)]
      //  key    value    key的解码方式   value的解码方式
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler)
    } else {
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicSet)
    }

    // (fromOffset, untilOffset)偏移量的范围(每个分区都有一个偏移量，一个RDD有多个分区)，直连方式每个Task直接连到Topic的一个分区上
    var offsetRanges = Array[OffsetRange]()

    // 从kafka读取的消息，DStream的Transform方法可以将当前批次的RDD获取出来
    // 该transform方法计算获取到当前批次RDD,然后将RDD的偏移量取出来，然后在将RDD返回到DStream
    val transform: DStream[(String, String)] = kafkaStream.transform(rdd => {
      // 得到该rdd对应kafka的消息的offset
      // 该RDD是一个KafkaRDD，可以获得偏移量的范围
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      //println(offsetRanges.length)
      rdd
    })

    val message: DStream[String] = transform.map(_._2)
    // 依次迭代DStream中的RDD
    message.foreachRDD(rdd => {
      // 对RDD进行操作，触发Action
      rdd.foreachPartition(it => {
        it.foreach(x => println(x))
      })

      // 更新偏移量
      for (o <- offsetRanges) {
        val zkPath = s"$offsetDir/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
