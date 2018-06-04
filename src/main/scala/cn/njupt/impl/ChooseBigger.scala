package cn.njupt.impl

class ChooseBigger {
  def choose1(b1: Boy, b2: Boy)(implicit ord: Boy => Ordered[Boy]): Boy = {
    if (b1 > b2) b1 else b2
  }

  /**
    * <%泛型视图限定符，表示把传入不是Comparable[T]类型的 隐式传换 为Comparable[T]类型
    * Comparable[T]:为T下界，T:为Comparable[T]上界
    */
  def choose2[T <% Ordered[T]](b1: T, b2: T): T = {
    if (b1 > b2) b1 else b2
  }

  def select1(b1: Boy, b2: Boy)(implicit ord: Ordering[Boy]): Boy = {
    if (ord.gt(b1, b2)) b1 else b2
  }

  /**
    * 上下文界定：上下文界定是隐式参数的语法糖。如：Ordering：可以进行隐式转化的T类型。
    * [T: Ordering]表示存在一个Ordering[T]的隐式值
    */
  def select2[T: Ordering](b1: T, b2: T): T = {
    val ord = implicitly[Ordering[T]]
    if (ord.gt(b1, b2)) b1 else b2
  }
}

object ChooseBigger {
  def main(args: Array[String]): Unit = {
    val b1 = new Boy("Tom", 20);
    val b2 = new Boy("Jerry", 25);
    val tool = new ChooseBigger

    import MyPreDef.boyToOrderedBoy
    println(tool.choose1(b1, b2).age)
    println(tool.choose2(b1, b2).age)

    import MyPreDef.BoyOrdering
    println(tool.select1(b1, b2).age)
    println(tool.select2(b1, b2).age)
  }
}
