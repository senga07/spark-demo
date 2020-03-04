package com.hlsijx.spark.sql.version_2

import java.sql.DriverManager

/**
  * 通过JDBC的方式访问ThriftServer
  */
object ThriftServerApp {
  def main(args: Array[String]): Unit = {

    Class.forName("org.apache.hive.jdbc.HiveDriver")

    val url = "jdbc:hive2://47.105.57.238:10000"
    val connection = DriverManager.getConnection(url, "root", "123456")
    val preparedStatement = connection.prepareStatement("select * from h_demo.emp")
    val resultSet = preparedStatement.executeQuery()

    while(resultSet.next()){
      val empno = resultSet.getInt("empno")
      val ename = resultSet.getString("ename")
      val salary = resultSet.getDouble("salary")
      println("empno=" + empno + ",ename=" + ename + ",salary=" + salary)
    }

    resultSet.close()
    preparedStatement.close()
    connection.close()
  }
}
