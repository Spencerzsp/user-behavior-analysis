package com.bgdata.flink.market

import java.sql.Timestamp
import java.util.UUID

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
  * @ author spencer
  * @ date 2020/7/20 10:57
  */

//定义输入数据的样例类
case class MarketUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

//定义输出数据的样例类
case class MarketCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)
//自定义测输入数据源
class SimulateMarketEventSource extends RichParallelSourceFunction[MarketUserBehavior]{
  //定义是否在运行的标识位
  var running: Boolean = true

  //定义用户行为和推广渠道的集合
  val behaviorSet: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  val channelSet: Seq[String] = Seq("appstore", "huaweistore", "weibo", "weichat")

  //定义随机数生成器
  val random = Random

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketUserBehavior]) = {
    //定义一个发出数据的最大量，用于控制测试数据量
    val maxCounts = Long.MaxValue
    var count = 0L

    //while循环，不停随机生成数据
    while (running && count < maxCounts){
      val id = UUID.randomUUID().toString
      val behavior = behaviorSet(random.nextInt(behaviorSet.size))
      val channel = channelSet(random.nextInt(channelSet.size))
      val ts = System.currentTimeMillis()

      ctx.collect(MarketUserBehavior(id, behavior, channel, ts))

      count += 1
      Thread.sleep(50L)
    }
  }
}

object AppMarketingByChannel {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketUserBehavior] = env.addSource(new SimulateMarketEventSource)
      .assignAscendingTimestamps(_.timestamp)

    val resultStream: DataStream[MarketCount] = dataStream.filter(_.behavior != "UNINSTALL")
      .keyBy(data => (data.channel, data.behavior))
      .timeWindow(Time.hours(1), Time.seconds(5))
      .process(new MarketCountByChannel())

    resultStream.print()

    env.execute("market count by channel")

  }
}

class MarketCountByChannel extends ProcessWindowFunction[MarketUserBehavior, MarketCount, (String, String), TimeWindow]{
  override def process(key: (String, String), context: Context, elements: Iterable[MarketUserBehavior], out: Collector[MarketCount]) = {
    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    val channel = key._1
    val behavior = key._2
    val count = elements.size

    out.collect(MarketCount(windowStart, windowEnd, channel, behavior, count))


  }
}
