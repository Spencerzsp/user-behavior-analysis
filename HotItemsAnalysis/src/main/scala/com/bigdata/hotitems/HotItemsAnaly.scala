package com.bigdata.hotitems

import java.lang
import java.sql.{Connection, PreparedStatement, Timestamp}
import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.commons.dbcp2.BasicDataSource
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ author spencer
  * @ date 2020/4/24 15:14
  */
// 定义输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义聚合窗口的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItemsAnaly {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.flink.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")

    val dataStream = env.addSource(new FlinkKafkaConsumer[String]("hot-items", new SimpleStringSchema(), properties))
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(
          dataArray(0).trim.toLong,
          dataArray(1).trim.toLong,
          dataArray(2).trim.toInt,
          dataArray(3).trim,
          dataArray(4).trim.toLong
      )
    }).assignAscendingTimestamps(_.timestamp)

//    val dataStream = env.readTextFile("D:\\IdeaProjects\\user-behavior-analysis\\HotItemsAnalysis\\src\\main\\resources\\user_behavior.csv")
//      .map(data => {
//        val dataArray = data.split(",")
//        UserBehavior(
//          dataArray(0).trim.toLong,
//          dataArray(1).trim.toLong,
//          dataArray(2).trim.toInt,
//          dataArray(3).trim,
//          dataArray(4).trim.toLong)
//      }).assignAscendingTimestamps(_.timestamp) //指定时间戳和watermark

//    val window = dataStream
//      .filter(_.behavior == "pv")
//      .keyBy("itemId")
//      .timeWindow(Time.hours(1), Time.minutes(5))
//      .aggregate()
//      .aggregate(new AggCount(), new WindowResult())
//      .aggregate(new AggCount, new WindowResultFunction)
//      .keyBy(_.windowEnd)
//      .process(new TopNHotItems(3))

//    window.print()

    // 根据pv获取热门商品item
    val processedStream = dataStream
      .filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.seconds(5))
//        .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
      .aggregate(new AggCount(), new WindowResult())  //窗口聚合，前面的输出是后面的输入
      .keyBy(_.windowEnd) //按照窗口分组
      .process(new TopNHotItems(3))

//    processedStream.addSink(new MySqlSinkFunction())

    processedStream.print()

    env.execute("hot item job")

  }
}

// 自定义预聚合函数
// 使用 .aggregate(AggregateFunction af, WindowFunction wf) 做增量的聚合操作，
// 它能使用AggregateFunction提前聚合掉数据，减少state的存储压力。较之 .apply(WindowFunction wf) 会将窗口中的数据都存储下来，
class AggCount() extends AggregateFunction[UserBehavior, Long, Long] {
  override def add(value: UserBehavior, accumulator: Long) = accumulator + 1

  override def createAccumulator() = 0L

  override def getResult(accumulator: Long) = accumulator

  override def merge(a: Long, b: Long) = a + b
}

// 自定义预聚合函数计算平均数
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double]{
  override def add(value: UserBehavior, accumulator: (Long, Int)) = (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def createAccumulator() = (0L, 0)

  override def getResult(accumulator: (Long, Int)) = (accumulator._1 / accumulator._2)

  override def merge(a: (Long, Int), b: (Long, Int)) = (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数，输出ItemViewCount
class WindowResult() extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
  }
}

// 自定义的处理函数
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{
  // 定义状态列表
  private var itemState: ListState[ItemViewCount] = _

  // 生命周期
  override def open(parameters: Configuration) = {
    // 定义一个状态描述器
    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(itemStateDesc)
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]) = {

    // 把每条数据存进状态列表
    itemState.add(value)

    // 注册一个定时器，windowEnd+1触发时说明window已经完成收集当前窗口所有数据
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器触发时，对所有数据排序并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]) = {
    // 将所有ListState中的数据取出，放入ListBuffer
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]()
    import scala.collection.JavaConversions._

    // 遍历
    for (item <- itemState.get()){
      allItems += item
//      allItems.append(item)
    }
    // 按照count大小排序,并取前N个
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    // 清空状态中的数据
    itemState.clear()

    // 格式化输出
    val result: StringBuilder = new StringBuilder()

    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    // 输出每一个商品的信息
    for (i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No").append(i + 1).append(":")
        .append(" 商品ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count)
        .append("\n")
    }

    result.append("===================")

    // 控制输出频率
    Thread.sleep(1000)

    out.collect(result.toString())
  }
}

class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: lang.Iterable[Long], out: Collector[ItemViewCount]) = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator().next()))
  }
}

class MySqlSinkFunction() extends RichSinkFunction[List[ItemViewCount]]{

  private var ps: PreparedStatement = _
  private var conn: Connection = _
  private var dataSource: BasicDataSource = _

  override def open(parameters: Configuration): Unit = {
    dataSource = new BasicDataSource()
    val conn: Connection = generateDataSource(dataSource)

    val sql = "insert into top3item values(null, ?, ?, ?, ?, ?)"

    conn.prepareStatement(sql)
  }

  override def invoke(value: List[ItemViewCount], context: SinkFunction.Context[_]): Unit = {

    for (itemViewCount <- value) {
//      ps.setString(1, itemViewCount.itemId)
    }
  }

  override def close(): Unit = {

  }

  def generateDataSource(dataSource: BasicDataSource): Connection = {
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    dataSource.setUrl("jdbc:mysql://wbbigdata01:3306/flink")
    dataSource.setUsername("root")
    dataSource.setPassword("bigdata")

    // 设置连接池的一些参数
    dataSource.setInitialSize(10)
    dataSource.setMaxTotal(50)
    dataSource.setMinIdle(2)

    try {
      conn = dataSource.getConnection()
      println("mysql创建连接池" + conn)
    }catch {
      case e: Exception =>
        e.printStackTrace()
    }

    return conn
  }
}

//class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
//  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
//    val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
//    val windowEnd: Long = window.getEnd
//    val count: Long = input.iterator.next()
//    out.collect(ItemViewCount(itemId, windowEnd, count))
//  }
//}
