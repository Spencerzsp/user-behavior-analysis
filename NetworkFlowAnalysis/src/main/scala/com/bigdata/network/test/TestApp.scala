package com.bigdata.network.test

import com.bigdata.network.utils.{DateUtils, StringUtils}

import scala.util.Random

/**
  * @ author spencer
  * @ date 2020/4/27 15:35
  */
object TestApp {

  def main(args: Array[String]): Unit = {
    val random = new Random()
    for (i <- 1 to 100){
      val ip = "192.168." + random.nextInt(255) + "." + random.nextInt(255)
      val userId = "-"
      val userName = "-"
      val visitTime = DateUtils.getTodayDate() + ":" +
        random.nextInt(23) + ":" +
        StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" +
        StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + " +0000"
      val requestTypes = Array("GET", "POST", "PUT")
      val requestType = requestTypes(random.nextInt(3))
      val urls = Array("weibo.com", "wechat.com", "qq.com", "alibaba.com", "storm.com", "spark.com", "flink.com", "hive.com", "hadoop.com", "hbase.com")
      val url = "/www/bigdata/spencer/" + urls(random.nextInt(10))

      println(ip + " " + userId + " " + userName + " " + visitTime + " " + requestType + " " + url)
    }
//
//    val visitTime = DateUtils.getTodayDate() + ":" + random.nextInt(23) + ":" +  StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
//    println(visitTime)


  }
}
