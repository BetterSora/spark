package cn.njupt

import java.io.File

import scala.io.{Source => io}

object MyDef {
  implicit def file2RichFile(path: File): RichFile = new RichFile(path)

  implicit val delimiters: Delimiters = Delimiters("<<", ">>")
}

class RichFile(path: File) {
  def read: String = io.fromFile(path.getPath).mkString
}

case class Delimiters(left: String, right: String)

/**
  * 要求存在一个类型为Ordering[T]的隐式值，该隐式值可以被用于该类的方法中
  */
class Pair[T: Ordering](val first: T, val second: T) {
  // 自动查找隐式值
  /*def smaller(implicit ord: Ordering[T]): T = if (ord.compare(first, second) < 0) first else second*/

  // 手动获取，调用了Predef.scala中的implicitly函数
  /*def smaller: T = if (implicitly[Ordering[T]].compare(first, second) < 0) first else second*/

  // 利用Ordered特质中定义的从Ordering到Ordered隐式转换
  def smaller: T = {
    import Ordered._
    if (first < second) first else second
  }
}

/**
  * 隐式转换在三种各不相同的情况下被考虑
  *
  * 1.表达式的类型与预期的类型不同
  * 2.对象访问一个不存在的成员
  * 3.对象调用某个方法而方法的声明与传入的参数不匹配
  */
object ImplicitTest {
  // 隐式参数，对于给定的数据类型只能有一个隐式值
  def quote(what: String)(implicit delims: Delimiters): String = delims.left + what + delims.right

  /*def smaller[T](a: T, b: T)(implicit order: T => Ordered[T]): T = if (order(a) < b) a else b*/
  // order既是隐式参数也是隐式转换
  def smaller[T](a: T, b: T)(implicit order: T => Ordered[T]): T = if (a < b) a else b

  def main(args: Array[String]): Unit = {
    println("========测试隐式转换========")
    // 引入隐式转换
    import MyDef._

    val result = new File("./output/part-00000").read
    println(result)

    println("========测试隐式参数========")
    // 显示调用
    println(quote("Hello")(Delimiters("<<", ">>")))

    // 隐式调用
    println(quote("Hello"))

    println("========利用隐式参数进行隐式转换========")
    println(smaller(3, 4))

    println("========上下文界定========")
    println(new Pair[Int](40, 3).smaller)
  }
}
