package com.bigdata.hotitems

import com.bigdata.hotitems.PvAnalysis.getClass
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * @ author spencer
  * @ date 2020/5/11 17:20
  */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val resource = getClass.getResource("/user_behavior.csv")
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(data => ("dumyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UvCountWithBloom())


    dataStream.print("uv with bloom job")
    env.execute()
  }
}

/**
  * 自定义窗口触发器
  */
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext) = {}

  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext) = {
    // 每来一条数据，就触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }
}

/**
  * 自定义布隆过滤器
  */
class Bloom(size: Long) extends Serializable{
  //位图的大小
  private val cap = if (size > 0) size else 1 << 27

  //定义哈希函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length){
      result = (value * seed + value.charAt(i)).toLong
    }
    result & (cap - 1)
  }
}


class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{

//  lazy val jedis = new Jedis("localhost", 6379)
//  jedis.auth("bigdata")
//  lazy val bloom = new Bloom(1 << 29)
  var jedis: Jedis = null
  var bloom: Bloom = null

  override def open(parameters: Configuration) = {
    // 定义redis连接
    jedis = new Jedis("localhost", 6379)
    jedis.auth("bigdata")
    bloom = new Bloom(1 << 29)
  }

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    val storeKey = context.window.getEnd.toString
    var count = 0L
    if (jedis.hget("count", storeKey) != null){
      jedis.hget("count", storeKey).toLong
    }

    // 用布隆过滤判断当前用户是否已经存在
    val userid = elements.last._2.toString
    val offset = bloom.hash(userid, 5)

    var isExist = jedis.getbit(storeKey, offset)
    if (!isExist){
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(storeKey.toLong, count + 1))
    }else{
      out.collect(UvCount(storeKey.toLong, count + 1))
    }
  }
}


