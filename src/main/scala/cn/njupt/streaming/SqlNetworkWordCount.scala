package cn.njupt.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming整合Spark SQL完成词频统计操作
  */
object SqlNetworkWordCount {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SqlNetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop", 6789)
    val words: DStream[String] = stream.flatMap(_.split(" "))

    words.foreachRDD((rdd, time) => {
      // 在Driver端执行的
      val spark = SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._

      val wordsDF: DataFrame = rdd.map(word => Record(word)).toDF()

      wordsDF.createOrReplaceTempView("words")

      println(s"======== $time ========")
      spark.sql("select word, count(*) as counts from words group by word").show()
    })

    ssc.start()
    ssc.awaitTermination()
  }

  case class Record(word: String)

  object SparkSessionSingleton {
    @transient  private var instance: SparkSession = _

    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
}


