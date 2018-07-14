package cn.njupt

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试Spark Join的宽依赖、窄依赖
  * CoGroupedRDD中的getDependencies方法
  */
object TestJoin {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestJoin").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array(("hello", 1), ("kitty", 3), ("hello", 1), ("kitty", 3)), 2)
    val rdd2 = sc.parallelize(Array(("hello", 1), ("kitty", 3), ("hello", 1), ("kitty", 3)), 2)
    // rdd.partitioner == Some(part) 上游没有分区器，所以这里判断为false
    println(rdd1.partitioner.getOrElse().toString)
    //rdd1.glom().foreach(tp => print(tp.toBuffer))
    //rdd2.glom().foreach(tp => print(tp.toBuffer))

    //val joined1 = rdd1.join(rdd2)
    //joined1.glom().foreach(tp => print(tp.toBuffer))

    val rdd3: RDD[(String, Iterable[Int])] = rdd1.groupByKey(2)
    val rdd4: RDD[(String, Iterable[Int])] = rdd2.groupByKey(2)
    // rdd.partitioner == Some(part) 此时上下游分区器相同且分区数量相等，所以这里判断为true
    val joined2: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd3.join(rdd4)
    joined2.collect()

    sc.stop()
  }
}
