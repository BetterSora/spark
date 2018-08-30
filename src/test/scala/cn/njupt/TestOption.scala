package cn.njupt

object TestOption {
  def main(args: Array[String]): Unit = {
    val a: Int = 3
    val b: Int = if (a == 3) 4 else 5

    //val someString: Option[String] = Some("hehe")
    val someString: Option[String] = None
    someString.map(println).getOrElse(println("haha"))

    //println("hehe")
  }
}
