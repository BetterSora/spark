package cn.njupt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 黑名单过滤
  */
object TransformApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TransformApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 黑名单 ("ls", true)
    val blackListRDD = ssc.sparkContext.parallelize(Array("zs", "ls")).map(x => (x, true))

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop", 6789)
    // "20180505, ls" => ("ls", "20180505, ls")
    val DS1: DStream[(String, String)] = stream.map(line => (line.split(",")(1), line))
    val DS2: DStream[String] = DS1.transform(rdd => {
      // ("ls", "20180505, ls") => ("ls", ("20180505, ls", true))
      rdd.leftOuterJoin(blackListRDD).filter(x => !x._2._2.getOrElse(false)).map(x => x._2._1)
    })

    DS2.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
