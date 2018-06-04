package cn.njupt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成有状态的统计(累计统计)
  */
object StatefulWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("StatefulWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 如果使用了stateful的算子，必须要设置checkpoint，底层调用的sc.setCheckpointDir
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop", 6789)
    val result: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1))

    // 根据key来更新数据
    val updateFunction: (Seq[Int], Option[Int]) => Option[Int] = (newValues, preValues) => {
      val newCount = newValues.sum
      val preCount = preValues.getOrElse(0)
      Some(newCount + preCount)
    }
    val state: DStream[(String, Int)] = result.updateStateByKey[Int](updateFunction)
    state.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
