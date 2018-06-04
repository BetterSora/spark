package cn.njupt.SQL

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

/**
  * 通过JDBC访问(相当于beeline去访问sever)
  * 先要启动server：start-thriftserver.sh --master local[2] --jars ~/apps/mysql-connector-java-5.1.27-bin.jar
  */
object SparkSQLThriftServerApp {
  def main(args: Array[String]): Unit = {
    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val connection: Connection = DriverManager.getConnection("jdbc:hive2://192.168.197.131:10000", "qinzhen", "")
    val pstmt: PreparedStatement = connection.prepareStatement("select * from default.src limit 10")
    val rs: ResultSet = pstmt.executeQuery()

    while (rs.next()) {
      val key: Int = rs.getInt("key")
      val value: String = rs.getString("value")

      println(s"key: $key value: $value")
    }

    rs.close()
    pstmt.close()
    connection.close()
  }
}
