package com.bigdata.flink

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @ author spencer
  * @ date 2020/6/15 16:55
  *
  * CEP处理订单失效timeout的问题：同一个订单id在15min内只有"create",没有"pay"，则为timeout
  */

//定义输入的订单事件流
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

//定义输出的结果
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val dataStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    //定义pattern
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
//        .next("flow")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    //定义一个输出标签，用于定义侧输出流
    val orderTimeoutOutput: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")

    val orderPayPatternStream = CEP.pattern(dataStream, orderPayPattern)

    import scala.collection.Map
    //从pattern stream中获取输出流
    val completedResultDataStream = orderPayPatternStream.select(orderTimeoutOutput)(
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val timeoutOrderId = pattern.getOrElse("begin", null).iterator.next().orderId
        OrderResult(timeoutOrderId, "timeout")
      }
    )(
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val payOrderId = pattern.getOrElse("follow", null).iterator.next().orderId
        OrderResult(payOrderId, "success")
      }
    )

    //打印正常输出匹配到的事件结果，timeout的事件结果需要在测输出流中获取
    completedResultDataStream.print()


    val timeoutResultDataStream = completedResultDataStream.getSideOutput(orderTimeoutOutput)

    //打印输出timeout的事件结果
    timeoutResultDataStream.print()

    env.execute("OrderTimeout")
  }
}
