package cn.njupt

import java.text.MessageFormat
import java.util

import scala.beans.BeanProperty
import scala.collection.immutable.SortedMap
import scala.collection.mutable.ArrayBuffer
import scala.io.{Source, StdIn}
import scala.reflect.runtime.universe._
import scala.collection.JavaConverters._
import scala.util.control.Breaks._

class Test {
  //private[this] var num: Int = _ // 只能当前对象访问
  private[Test] var num: Int = _ // 该类的外部类，当前类以及当前类的伴生对象可以访问
  @BeanProperty
  var age = 10

  def fun2(obj: Test): Unit = {
    println(obj.num)

  }

  // 传入的类必须有的方法
  def meet(p: {def greeting: String}): Unit = {
    println("meet调用成功 " + p.greeting)

    if (3 > 4) break()
  }
}

class Test2 {
  def func(): Unit = {
    //new Test().num // 访问不了
  }
}

object Test {
  def func(str: String): Unit = {
    println(str)
    println(new Test().num)
    //2 编译器判断结果返回不是Unit类型的话，自动在最后返回()
  }

  def >>:(data: String): Test.type = {
    println(data)
    Test
  }

  def sum(args: Int*): Unit = {
    //println(args.head)
    println(args)
  }

  // 产生一个Stream
  def numsFrom(n: Int): Stream[Int] = n #:: numsFrom(n + 1)

  // 如果有return就没有自动推断功能了，要手动加上返回值类型
  def funcc1():Int = return 3
  def funcc2() = 3

  // Manifest上下文界定
  def makePair[T : Manifest](first: T, second: T): Unit = {
    Array[T](first, second)
    /*val r = new Array[T](2)
    r(0) = first
    r(1) = second*/
  }

  // 类型推断：输入List(1,2,3)时无法推断出A为Int
  def firstLast[A, B <: Iterable[A]](it: B): (A, A) = {
    (it.head, it.last)
  }

  // 可以推断
  def firstLastNew[A, B](it: B)(implicit ev: B <:< Iterable[A]): (A, A) = {
    (it.head, it.last)
  }

  def main(args: Array[String]): Unit = {
    new Test().fun2(new Test())
    Test func "1"
    func {"11"}

    /*val name = StdIn.readLine("Your name: ")
    println(name)*/

    sum(1,2,3,4,5)
    sum(1 to 10: _*)

    val str = MessageFormat.format("The answer to {0} is {1}", "everything", 42.asInstanceOf[AnyRef])
    println(str)

    /*lazy val words = Source.fromFile("").mkString*/

    val buffer = ArrayBuffer(1, 2, 3, 4, 5)
    /*val buffer = Array(1, 2, 3, 4, 5)*/
    val newBuffer = for (elem <- buffer) yield elem * 2 // 创建一个与原类型相同的集合
    println(newBuffer)

    val temp = newBuffer filter {_ % 2 == 0} map {x => x + 1}
    println(temp.mkString("<", ",", ">"))

    val a: PartialFunction[Int,String] = { case 2 => "OK" }
    println(a)
    val b = (x: Int) => x + 10
    println(b)
    println(b(10))

    "myData" >>: "myName" >>: Test

    println(classOf[List[Int]] == classOf[List[String]])
    println(typeOf[List[Int]] == typeOf[List[String]])
    println(Test.getClass)
    println(new Test().getClass)

    val map = new java.util.TreeMap[String, Int]().asScala
    map("Alice") = 10
    map("Alice") = 5
    println(map)

    val t = new Test
    t.age_=(25) // 调用setter方法
    println(t.age) // 调用getter方法
    t.getAge

    new Test().meet(new Test2() {
      def greeting: String = {
        "hello"
      }
    })
    //println(StdIn.readInt())

    // 简写方式只在参数类型已知的情况下有效
    // 当你将一个匿名函数传递给另一个函数或方法时，Scala会尽可能帮你推断出类型信息
    // 如果参数只在=>右侧出现一次，你可以用_替换掉它
    val fun1 = 3 * (_: Double)
    val fun2 = (x: Double) => 3 * x
    val fun3: Double => Double = 3 * _

    var x = 10
    def myUntil(condition: Boolean)(block: => Unit): Unit = {
      if (!condition) {
        block
        myUntil(x == 0)(block)
      }
    }

    myUntil(x == 0) {
      x -= 1
      println(x)
    }

    println(numsFrom(10).take(5).force)
    println(numsFrom(10)(5))

    // 懒视图
    (0 to 100).view.map(math.pow(10, _)).map(1 / _).force

    // 并行集合
    for (i <- (0 until 100).par) println(i + " ")

    // 偏函数
    val f: PartialFunction[String, Int] = { case "+" => 1; case "-" => -1; case "*" => 0}

    makePair(new Test, new Test)
    makePair(1, 2)
  }
}

/**
  * 抽象类
  */
abstract class Person(val name: String) {
  def doSomething(): String
}

class Student(val id: Int, name: String) extends Person(name) {
  //val doSomething = "hi"
  override def doSomething(): String = {
    "Say Hello"
  }
}

object Student extends App {
  val student = new Student(1, "zhangsan")
  println(student.doSomething())
}

/**
  * 构造顺序和提前定义
  */
class Creature {
  val range = 10
  //lazy val range = 10
  val env = new Array[Int](range)
  println("--" + env.length + "--")
}

/*class Ant extends Creature {
  //override val range = 2
  override lazy val range = 2
  println("===")
}*/

/**
  * 提前定义：可以在超类的构造器之前初始化子类的val字段
  */
class Ant extends {override val range = 2} with Creature {
  println("===")
}

object Ant extends App {
  println(new Ant().env.length)
}

// 带有特质的对象
trait Log {
  def log(): Unit = {}
}

trait Logger1 extends Log {
  override def log(): Unit = {
    println("Logger1")
  }
}

trait Logger2 extends Log {
  override def log(): Unit = {
    println("Logger2")
  }
}

class LogClass extends Log {
  def test(): Unit = {
    log()
  }
}

object LogClass extends App {
  val log1 = new LogClass() with Logger1
  val log2 = new LogClass() with Logger2
  log1.test()
  log2.test()
}

class myClass(f: (Int, Int, String) => String) {
  def compute(): String = {
    f(1, 2, "Hello")
  }
}

object myClass {
  def main(args: Array[String]): Unit = {
    val a = new myClass((a, b, c) => a + b + c)
    println(a.compute())
  }
}

object Name {
  /*def unapply(name: String): Option[(String, String)] = {
    val fields = name.split(" ")
    if (fields.length != 2)
      None
    else
      Some(fields(0), fields(1))
  }*/

  def unapplySeq(input: String): Option[Seq[String]] = {
    // 隐式转换将Array转成WrappedArray，WrappedArray继承了AbstractSeq
    if (input.trim == "") None else Some(input.trim.split("\\s+"))
  }
}

object TestName {
  def main(args: Array[String]): Unit = {
    val people = StdIn.readLine()
    /*val Name(first, last) = people
    println(first + "-----" + last)*/

    people match {
      case Name(first, last) => println(first + "-----" + last)
      case Name(first, middle, last) => println(first + "-----" + middle + "-----" + last)
      case Name(first, "van", "der", last) => println(first + "-----" + "van" + "----" + "der" + "-----" + last)
    }
  }
}

// 匹配嵌套结构
abstract class Item
case class Article(description: String, price: Double) extends Item
case class Bundle(description: String, discount: Double, items: Item*)
object TestItem {
  def main(args: Array[String]): Unit = {
    val temp = Bundle("Father's day special", 20.0,
      Article("Scala", 39.95),
      Article("Java", 45.88),
      Article("Python", 32.98))

    temp match {
      case Bundle(_, _, Article(desc, _), _*) => println(desc)
      case Bundle(_, _, art @ Article(_, _), rest @ _*) => println(art, rest)
    }
  }
}

// this返回的是对象,this.type返回的是类型
object Title
class Document {
  def set(obj: Title.type): this.type = this
  def to(arg: String): Unit = println(arg)
}
object TestDocument {
  def main(args: Array[String]): Unit = {
    val book = new Document
    book set Title to "Scala for the Impatient"
  }
}