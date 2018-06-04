package cn.njupt.streamingProj.spark

import cn.njupt.streamingProj.dao.{CourseClickCountDAO, CourseSearchClickCountDAO}
import cn.njupt.streamingProj.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import cn.njupt.streamingProj.utils.DateUtils
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

/**
  * 使用Spark Streaming处理Kafka过来的数据
  */
object StatStreamingApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()//.setAppName("StatStreamingApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(60))

    val kafkaParams = Map[String, String]("metadata.broker.list" -> "hadoop:9092")
    val topics = Set("hello_topic")
    val kafkaStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

    val logs: DStream[String] = kafkaStream.map(_._2)
    // 测试数据接收
    //logs.count().print()
    // 数据清洗
    // 63.30.29.55	2018-05-12 09:33:03	"GET /class/131.html HTTP/1.1"	200	http://cn.bing.com/search?q=大数据面试
    // ip time courseId statusCode referer
    val cleanData = logs.map(line => {
      val fields = line.split("\t")
      val ip = fields(0)
      val time = DateUtils.parse(fields(1))
      val url = fields(2).split(" ")(1)
      var courseId = 0

      // 获取课程编号
      if (url.startsWith("/class")) {
        courseId = url.split("/")(2).split("\\.")(0).toInt
      }

      val statusCode = fields(3).toInt
      val referer = fields(4)

      ClickLog(ip, time, courseId, statusCode, referer)
    }).filter(clickLog => clickLog.courseId != 0)
    // 测试数据清洗
    cleanData.print()

    // 统计今天到现在为止实战课程的访问量  HBase RowKey: 20180512_88
    cleanData.map(clickLog => {
      (clickLog.time + "_" + clickLog.courseId, 1)
    }).reduceByKey(_ + _).foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val list = new ListBuffer[CourseClickCount]

        iter.foreach(info => {
          list.append(CourseClickCount(info._1, info._2))
        })

        // 将数据写入HBase
        CourseClickCountDAO.save(list)
      })
    })

    // 统计从搜索引擎过来的今天到现在为止实战课程的访问量
    cleanData.map(clickLog => {

      /**
        * https://www.sogou.com/web?query=Spark SQL实战
        *
        * ==>
        *
        * https:/www.sogou.com/web?query=Spark SQL实战
        */
      val referer = clickLog.referer.replaceAll("//", "/")
      val fields = referer.split("/")
      var host = ""
      if (fields.length > 2) {
        host = fields(1)
      }
      val time = clickLog.time.substring(0, 8)
      val id = clickLog.courseId

      (host, id, time)
    }).filter(_._1 != "").map(info => (info._3 + "_" + info._1 + "_" + info._2, 1)).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          val list = new ListBuffer[CourseSearchClickCount]

          iter.foreach(info => {
            list.append(CourseSearchClickCount(info._1, info._2))
          })

          // 将数据写入HBase
          CourseSearchClickCountDAO.save(list)
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
