package com.bigdata.network.mock

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * @ author spencer
  * @ date 2020/4/24 15:37
  */
object Kafka2Consumer {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val topic = "network-flow"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "dafa1:9090")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val inputStream = env.addSource(new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties))
//    inputStream.writeAsCsv("D:\\IdeaProjects\\user-behavior-analysis\\HotItemsAnalysis\\src\\main\\resources\\user_behavior.csv")

    inputStream.print()
    env.execute()
  }

}
