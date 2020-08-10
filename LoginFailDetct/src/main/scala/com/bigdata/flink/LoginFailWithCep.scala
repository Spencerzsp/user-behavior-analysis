package com.bigdata.flink

import java.util

import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, SimpleCondition}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

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

//    其它定义pattern的几种方式
//    Pattern.begin[LoginEvent]("start")
//      .where(new IterativeCondition[LoginEvent] {
//        override def filter(t: LoginEvent, context: IterativeCondition.Context[LoginEvent]): Boolean = t.eventType == "fail"
//      })
//
//    val pattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("start")
//      .where(new SimpleCondition[LoginEvent] {
//        override def filter(value: LoginEvent): Boolean = value.eventType == "fail"
//      })
//      .next("next")
//      .where(new SimpleCondition[LoginEvent] {
//        override def filter(value: LoginEvent): Boolean = value.eventType == "fail"
//      })
//      .times(2)
//      .within(Time.seconds(2))
//
//    val patternStream: PatternStream[LoginEvent] = CEP.pattern(dataStream, pattern)
//    val value: DataStream[(String, String, String)] = patternStream.process(new PatternProcessFunction[LoginEvent, (String, String, String)] {
//      override def processMatch(map: util.Map[String, util.List[LoginEvent]], context: PatternProcessFunction.Context, collector: Collector[(String, String, String)]): Unit = {
//        val iter: util.Iterator[LoginEvent] = map.get().iterator()
//        while (iter.hasNext) {
//
//          val loginEvent: LoginEvent = iter.next()
//          collector.collect((loginEvent.ip, loginEvent.userId.toString, loginEvent.eventType))
//        }
//      }
//    })

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

//    logFailDataStream.print()

    env.execute("LoginFailWithCep")
  }

}
