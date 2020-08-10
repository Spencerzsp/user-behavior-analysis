package com.bigdata.flink

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author spencer
  * @ date 2020/6/15 16:27
  */
object LoginFailWithCep {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val dataStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430832),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845),
      LoginEvent(2, "192.168.10.11", "fail", 1558430846),
      LoginEvent(2, "192.168.10.12", "fail", 1558430847)

    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.userId)

    //定义一个匹配模式,next紧邻发生的事件,判断在2s内紧邻发生的事件是否为fail
    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType == "fail")
      .next("next")
      .where(_.eventType == "fail")
      .times(2)
      .within(Time.seconds(2))

    //在keyBy之后的流中匹配出pattern stream
    val loginPatternStream = CEP.pattern(dataStream, loginFailPattern)

    import scala.collection.Map
    val logFailDataStream = loginPatternStream.select(
      (pattern: Map[String, Iterable[LoginEvent]]) => {
//        val begin = pattern.getOrElse("begin", null).iterator.next()
        val next = pattern.getOrElse("next", null).iterator.next()
        (next.userId, next.ip, next.eventType)
      }
    )

    logFailDataStream.print()

    env.execute("LoginFailWithCep")
  }

}
