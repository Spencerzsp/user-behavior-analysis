package com.bigdata.flink

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ author spencer
  * @ date 2020/6/15 10:34
  */

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LogFailAnalysis {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val loginEventStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process(new MatchFunction())
      .print()

    env.execute("LogFailAnalysis")
  }
}

class MatchFunction() extends KeyedProcessFunction[Long, LoginEvent, LoginEvent]{

  private var logState: ListState[LoginEvent] = _
  override def open(parameters: Configuration) = {

    logState  = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent](
      "log-state",
      classOf[LoginEvent]
    ))

  }

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]) = {

    logState.add(value)

    //注册定时器，触发时间确定为2s后
    ctx.timerService().registerEventTimeTimer(value.eventTime + 2 * 1000)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]) = {

    val allLogins = ListBuffer[LoginEvent]()

//    import scala.collection.JavaConversions._
//
//    for (login <- logState.get){
//      allLogins += login
//    }

    val iter = logState.get().iterator()
    while (iter.hasNext){
      allLogins.append(iter.next())
    }

    logState.clear()

    if (allLogins.length > 1){
      out.collect(allLogins.head)
    }
  }
}
