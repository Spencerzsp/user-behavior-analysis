package com.bigdata.test

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.streaming.api.scala._

/**
  * @ author spencer
  * @ date 2020/4/27 11:40
  */
object TestApp {
  def main(args: Array[String]): Unit = {
    val timestamp: Long = System.currentTimeMillis()

    println(new Timestamp(timestamp - 1))

//    val cap = 1 << 3
//
//    println(1 & (cap - 1))
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//
//    env.execute("TestApp")
  }
}
