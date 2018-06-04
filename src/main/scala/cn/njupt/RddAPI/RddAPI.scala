package cn.njupt.RddAPI

import org.apache.spark.{SparkConf, SparkContext}

object RddAPI {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("API").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val func = (index: Int, iter: Iterator[Int]) => {
      iter.toList.map(x => s"[partition: $index, val: $x]").iterator
    }
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8 ,9), 2) // [partition: 0, val: 1]
    // mapPartitionsWithIndex: 是transformation操作，把每个partition中的分区号和对应的值拿出来
    println(rdd1.mapPartitionsWithIndex(func).collect().toBuffer)

    // aggregate: 是action操作, 第一个参数是初始值, 第二个参数是传两个函数，一个是分区内操作，一个是分区间操作，每次操作都要用到初始值
    val rdd2 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)
    println(rdd2.aggregate(0)(_ + _, _ + _)) // 45
    println(rdd2.aggregate(10)(_ + _, _ + _)) // 75
    println(rdd2.aggregate(5)(math.max(_, _), _ + _)) // 19
    println(rdd2.aggregate("")(_ + _, _ + _)) // 123456789或者567891234
    println(rdd2.aggregate("=")(_ + _, _ + _)) // ==1234=56789或者==56789=1234
    val rdd3 = sc.parallelize(List("12", "23", "345", "4567"), 2)
    println(rdd3.aggregate("")((x: String, y: String) => math.max(x.length, y.length).toString, (x: String, y: String) => x + y)) // 42或者24

    // aggregateByKey: 是transformation操作，和aggregate不同，初始值只会在分区内函数中被应用，区间合并时不用初始值
    val rdd4 = sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)
    println(rdd4.aggregateByKey(0)(_ + _, _ + _).collect().toBuffer) // (dog,12), (cat,19), (mouse,6)
    println(rdd4.aggregateByKey(5)(_ + _, _ + _).collect().toBuffer) // (dog,17), (cat,29), (mouse,16)
    println(rdd4.aggregateByKey(5)(math.max(_, _), _ + _).collect().toBuffer)// (dog,12), (cat,17), (mouse,10)

    // repartition: 是transformation操作，对数据进行shuffle
    // 如果重分区的数目大于原来的分区数，那么必须指定shuffle参数为true，否则，分区数不便
    val rdd5 = sc.parallelize(1 to 10, 10)
    // [partition: 0, val: 1], [partition: 1, val: 2], [partition: 2, val: 3],
    // [partition: 3, val: 4], [partition: 4, val: 5], [partition: 5, val: 6],
    // [partition: 6, val: 7], [partition: 7, val: 8], [partition: 8, val: 9], [partition: 9, val: 10]
    println(rdd5.mapPartitionsWithIndex(func).collect().toBuffer)
    // [partition: 0, val: 4], [partition: 0, val: 9], [partition: 0, val: 10],
    // [partition: 1, val: 1], [partition: 1, val: 2], [partition: 2, val: 7],
    // [partition: 2, val: 8], [partition: 3, val: 5], [partition: 3, val: 6], [partition: 4, val: 3]
    println(rdd5.repartition(5).mapPartitionsWithIndex(func).collect().toBuffer)
    // [partition: 3, val: 1], [partition: 3, val: 2], [partition: 3, val: 3], [partition: 3, val: 4],
    // [partition: 3, val: 5], [partition: 3, val: 6], [partition: 3, val: 7], [partition: 3, val: 8],
    // [partition: 3, val: 9], [partition: 3, val: 10]
    println(rdd5.coalesce(4, true).mapPartitionsWithIndex(func).collect().toBuffer)

    // collectAsMap: 是action操作, Map(b -> 2, a -> 5)，相同的key会更新value
    val rdd6 = sc.parallelize(List(("a", 1), ("b", 2), ("a", 5)))
    println(rdd6.collectAsMap)

    // combineByKey: 是transformation操作，和aggregate不同，初始值只会在分区内函数中被应用，区间合并时不用初始值
    // 第一个函数是每个分区中每个key中value中的第一个值
    // 当input下有3个文件时(有3个block块, 不是有3个文件就有3个block, ), 每个会多加3个10，相当于分了9个区
    val rdd7 = sc.parallelize(List(("a", 1), ("b", 2), ("a", 1), ("a", 1)), 2)
    println(rdd7.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n).collectAsMap()) // Map(b -> 2, a -> 3)
    println(rdd7.combineByKey(x => x + 10, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n).collectAsMap()) // Map(b -> 12, a -> 23)
    val rdd8 = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val rdd9 = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val rdd10 = rdd9.zip(rdd8)
    // (1,List(dog, cat, turkey)), (2,List(gnu, salmon, rabbit, wolf, bear, bee))
    println(rdd10.combineByKey(List(_), (x: List[String], y: String) => x :+ y, (m: List[String], n: List[String]) => m ++ n).collect().toBuffer)

    // countByKey, countByValue: 是action操作，countByValue是把整个元祖当成value!!!!!!
    val rdd11 = sc.parallelize(List(("a", 1), ("b", 2), ("b", 2), ("c", 2), ("c", 1)))
    println(rdd11.countByKey) // Map(a -> 1, b -> 2, c -> 2)
    println(rdd11.countByValue) // Map((c,2) -> 1, (a,1) -> 1, (b,2) -> 2, (c,1) -> 1)

    // filterByRange: 是action操作，前闭后闭
    val rdd12 = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2), ("a", 1)))
    // ArrayBuffer((c,3), (d,4), (c,2))
    println(rdd12.filterByRange("b", "d").collect().toBuffer)

    // flatMapValues: 是transformation操作
    val rdd13 = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
    println(rdd13.flatMapValues(_.split(" ")).collect().toBuffer) // ArrayBuffer((a,1), (a,2), (b,3), (b,4))

    // foldByKey: 是transformation操作
    val rdd14 = sc.parallelize(List("dog", "wolf", "cat", "bear"), 2)
    println(rdd14.map(x => (x.length, x)).foldByKey(" ")(_ + _).collect().toBuffer) // ArrayBuffer((4, wolf bear), (3, dog cat))

    // foreachPartition和map差不多，但是它不会生成新的集合，map是取出一个值，而这个是取出一个迭代器
    val rdd15 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 3)
    rdd15.foreachPartition((x: Iterator[Int]) => println(x.reduce(_ + _))) // 6 15 24
    println(rdd15.collect().toBuffer)

    // keyBy: 是transformation操作，以传入的参数做key
    val rdd16 = sc.parallelize(List("dog", "salmon", "salmon", "rat", "elephant"), 3)
    println(rdd16.keyBy((x: String) => x.length).collect().toBuffer) // ArrayBuffer((3,dog), (6,salmon), (6,salmon), (3,rat), (8,elephant))

    // keys： 是transformation操作，获取所有的键
    // values：是transformation操作，获取所有的值
    val rdd17 = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2).map((x: String) => (x, x.length))
    println(rdd17.keys.collect().toBuffer) // ArrayBuffer(dog, tiger, lion, cat, panther, eagle)
    println(rdd17.values.collect().toBuffer) // ArrayBuffer(3, 5, 4, 3, 7, 5)
  }
}
