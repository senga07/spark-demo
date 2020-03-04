package com.hlsijx.spark.sql.version_2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * DataFrame基本API操作
  */
object DataFrameApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("DataFrameApp").master("local[2]").getOrCreate()

//    employeeCase(sparkSession)

    empWithDeptCase(sparkSession)

    sparkSession.stop()
  }


  /**
    * 员工及部门案例，使用了隐式转换的方式将RDD转换为DataFrame
    * @param sparkSession 全局入口点
    */
  private def empWithDeptCase(sparkSession: SparkSession) = {

    val emp = sparkSession.sparkContext.textFile("./test-data/txt/emp.txt")
    val empDataFrame = getEmpDataFrame(sparkSession, emp)
    SingleEmpCase(empDataFrame)

    val dept = sparkSession.sparkContext.textFile("./test-data/txt/dept.txt")
    val deptDataFrame = getDeptDataFrame(sparkSession, dept)

    //select * from emp inner join dept on emp.deptno = dept.deptno limit 20;
    empDataFrame.join(deptDataFrame, empDataFrame("deptno") === deptDataFrame("deptno")).show()
  }

  private def SingleEmpCase(empDataFrame: DataFrame) = {

    //select * from emp limit 25;
    empDataFrame.show(25, false)

    //select * from emp where managerid = '' or allowance = '' limit 20;
    empDataFrame.filter("managerid='' OR allowance=''").show()

    //select * from emp where ename like 'S%' limit 20;
    empDataFrame.filter("SUBSTR(ename,0,1)='S'").show()

    //select * from emp order by ename asc, empno desc;
    empDataFrame.sort(empDataFrame("ename"), empDataFrame("empno").desc).show()
  }

  private def getEmpDataFrame(sparkSession: SparkSession, emp: RDD[String]) : DataFrame = {
    import sparkSession.implicits._
    emp.map(_.split("\\|"))
      .map(lines =>
        Emp(
          lines(0).toInt,
          lines(1),
          lines(2),
          lines(3),
          lines(4),
          lines(5),
          lines(6),
          lines(7).toInt
        )
      ).toDF()
  }

  case class Emp(empno : Int, ename : String, position : String, managerid : String,
                 hiredate : String, salary : String, allowance : String, deptno : Int)

  private def getDeptDataFrame(sparkSession: SparkSession, emp: RDD[String]) : DataFrame = {
    import sparkSession.implicits._
    emp.map(_.split("\\|"))
      .map(lines =>
        Dept(
          lines(0).toInt,
          lines(1),
          lines(2)
        )
      ).toDF()
  }
  case class Dept(deptno : Int, dname : String, city : String)
  /**
    * 员工表案例，实现了简单的api操作
    * 通过直接读取json文件的方式加载到DataFrame
    * @param sparkSession 全局入口点
    */
  private def employeeCase(sparkSession: SparkSession) = {

    val dataFrame = sparkSession.read.json("./test-data/json/employees.json")

    //select name, salary+10 as salary from employees limit 20;
    dataFrame.select(dataFrame.col("name"), (dataFrame.col("salary") + 10).as("salary")).show()

    //select * from employees where salary > 3000 limit 20;
    dataFrame.filter(dataFrame.col("salary") > 3000).show()

    //select count(*) from employees group by deptno limit 20;
    dataFrame.groupBy("deptno").count().show()
  }

}
