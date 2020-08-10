package com.bigdata.network.analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.bigdata.network.constant.Constant
import com.bigdata.network.utils.ReadConfigUtils
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ author spencer
  * @ date 2020/4/27 16:18
  */
// 输入数据样例类
case class ApacheEventLog(ip: String, userId: String, eventTime: Long, method: String, url: String)
// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkFlowAnalysis {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty(Constant.BOOTSTRAP_SERVERS, ReadConfigUtils.config.getString(Constant.BOOTSTRAP_SERVERS))
    properties.setProperty(Constant.GROUP_ID, ReadConfigUtils.config.getString(Constant.GROUP_ID))
    properties.setProperty(Constant.KEY_DESERIALIZER, ReadConfigUtils.config.getString(Constant.KEY_DESERIALIZER))
    properties.setProperty(Constant.VALUE_DESERIALIZER, ReadConfigUtils.config.getString(Constant.VALUE_DESERIALIZER))
    properties.setProperty(Constant.AUTO_OFFSET_RESET, ReadConfigUtils.config.getString(Constant.AUTO_OFFSET_RESET))

    // 获取kafka数据源
    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("network-flow", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray = data.split(" ")
        // 定义时间转换模板，将时间转换成时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime

        ApacheEventLog(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      .filter(_.method == "GET")
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheEventLog](Time.seconds(10)) {
        override def extractTimestamp(element: ApacheEventLog): Long = element.eventTime
      })
//      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))  //开窗，1min的窗口，每5秒滑动一次
      .allowedLateness(Time.seconds(60))   // 允许60s的迟到数据
      .aggregate(new CountAgg(), new WindowResult())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))

    dataStream.print()

    env.execute("network flow job")

  }

}

class CountAgg() extends AggregateFunction[ApacheEventLog, Long, Long]{
  override def add(value: ApacheEventLog, accumulator: Long) = accumulator + 1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a + b
}

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{

  //直接定义状态变量，懒加载
  lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]) = {

    //把数据加入到urlState
    urlState.add(value)

    //注册定时器，当windowEnd+10时触发窗口计算
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 10 * 1000) //延迟的wartermark保持一致
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]) = {

    val allUrl: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val iter = urlState.get().iterator()
    while (iter.hasNext){
      allUrl.append(iter.next())
//      allUrl += iter.next()
    }

    urlState.clear()

    val sortedUrlViews = allUrl.sortWith(_.count > _.count).take(topSize)
//    allUrl.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 格式化输出
    val result: StringBuilder = new StringBuilder()

    result.append("时间：").append(new Timestamp(timestamp - 10 * 1000)).append("\n")

    // 输出每一个商品的信息
    for (i <- sortedUrlViews.indices){
      val currentUrl = sortedUrlViews(i)
      result.append("No").append(i + 1).append(":")
        .append(" URL=").append(currentUrl.url)
        .append(" 访问量=").append(currentUrl.count)
        .append("\n")
    }

    result.append("=========================================================")

    // 控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}
