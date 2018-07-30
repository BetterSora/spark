package cn.njupt.SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * 用户定义的聚合函数（User-defined aggregate functions, UDAF）同时处理多行
  * 并且返回一个结果，通常结合使用 GROUP BY 语句（例如 COUNT 或 SUM）
  *
  * 为了简单起见，我们将实现一个叫 SUMPRODUCT 的 UDAF 来计算以库存来分组的所有车辆零售价值
  */
object ScalaUDAFExample {
  private[SQL] class SumProductAggregateFunction extends UserDefinedAggregateFunction {
    /**
      * 该方法指定具体输入数据的类型
      */
    override def inputSchema: StructType = new StructType().add("price", DoubleType).add("quantity", LongType)

    /**
      * 在进行聚合操作的时候所要处理的数据的结果的类型
      */
    override def bufferSchema: StructType = new StructType().add("total", DoubleType)

    /**
      * 指定UDAF函数计算后返回的结果类型
      */
    override def dataType: DataType = DoubleType

    /**
      * 给定一个input，UDAF的output是确定性的
      */
    override def deterministic: Boolean = true

    /**
      * 在Aggregate之前每组数据的初始化结果
      */
    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer.update(0, 0.0)

    /**
      * 在进行聚合的时候，每当有新的值进来，对分组后的聚合如何进行计算
      * 本地的聚合操作，相当于Hadoop MapReduce模型中的Combiner
      */
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum = buffer.getDouble(0)
      val price = input.getDouble(0)
      val quantity = input.getLong(1)

      buffer.update(0, sum + price * quantity)
    }

    /**
      * 最后在分布式节点进行Local Reduce完成后需要进行全局级别的Merge操作
      */
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))

    /**
      * 返回最终结果
      */
    override def evaluate(buffer: Row): Any = buffer.getDouble(0)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Scala UDAF Example").setMaster("local[2]")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    val df = spark.read.json("./src/main/resources/inventory.json")
    df.show()
    //assert(false)
    df.createOrReplaceTempView("inventory")
    spark.udf.register("SUMPRODUCT", new SumProductAggregateFunction)

    spark.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake FROM inventory GROUP BY Make").show()

    spark.stop()
  }
}
