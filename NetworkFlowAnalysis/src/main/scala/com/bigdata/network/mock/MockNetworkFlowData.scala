package com.bigdata.network.mock

import java.util.Properties

import com.bigdata.network.utils.{DateUtils, StringUtils}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockNetworkFlowData {

  def createKafkaProducer(brokers: String): KafkaProducer[String, String] = {

    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](prop)
  }

  def generateMockDate() = {

    val array = ArrayBuffer[String]()

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

      array += ip + " " + userId + " " + userName + " " + visitTime + " " + requestType + " " + url

    }

    array.toArray
  }

  def main(args: Array[String]): Unit = {

    val brokers = "dafa1:9090"
    val topics = "network-flow"

    val kafkaProducer = createKafkaProducer(brokers)

    while (true){
      for (item <- generateMockDate()) {
        kafkaProducer.send(new ProducerRecord[String, String](topics, item))
        println("正在发送数据到kafka集群...")
      }

      Thread.sleep(1000)
    }
  }

}
