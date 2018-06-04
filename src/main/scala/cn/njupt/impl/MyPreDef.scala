package cn.njupt.impl

/**
  * 定义隐式转换规则
  */
object MyPreDef {
  implicit val boyToOrderedBoy = (b1: Boy) => new Ordered[Boy] {
    override def compare(b2: Boy): Int = b1.age - b2.age
  }

  /*implicit object BoyOrdering extends Ordering[Boy] {
    override def compare(b1: Boy, b2: Boy): Int = b1.age - b2.age
  }*/

  implicit val BoyOrdering = new Ordering[Boy] {
    override def compare(b1: Boy, b2: Boy): Int = b1.age - b2.age
  }
}
