package com.hlsijx.spark.sql.utils

import java.util.regex.{ Pattern}

/**
  * 正则工具
  */
object RegExpUtils {

  def isPass(str : String, reg : String ): Boolean ={
    val pattern = Pattern.compile(reg)
    val matcher = pattern.matcher(str)
    matcher.matches()
  }

  def main(args: Array[String]): Unit = {
    print(isPass("http://www.imooc.com/article/11325", "[a-z:./]*article/[0-9]*"))
  }
}
