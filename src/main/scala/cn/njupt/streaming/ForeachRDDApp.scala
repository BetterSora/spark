package cn.njupt.streaming

import java.sql.{Connection, DriverManager}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 使用Spark Streaming完成词频统计，并将结果写入到MySQL数据库中
  */
object ForeachRDDApp {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ForeachRDDApp").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val stream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop", 6789)
    val result: DStream[(String, Int)] = stream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)
    result.print()

    result.foreachRDD((rdd: RDD[(String, Int)]) => {
      // 不能在这里建立连接，否则会出现序列化异常错误，因为是在Driver端创建了连接
      rdd.foreachPartition(iter => {
        val connection = getConnection
        connection.setAutoCommit(false)
        val pstmt = connection.prepareStatement("insert into wordcount(word, wordcount) values(?, ?)")
        iter.foreach(record => {
          pstmt.setString(1, record._1)
          pstmt.setInt(2, record._2)
          pstmt.addBatch()
        })
        pstmt.executeBatch()
        connection.commit()

        pstmt.close()
        connection.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 获取数据库连接
    */
  def getConnection: Connection = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://hadoop:3306/wc", "root", "root")
  }
}
