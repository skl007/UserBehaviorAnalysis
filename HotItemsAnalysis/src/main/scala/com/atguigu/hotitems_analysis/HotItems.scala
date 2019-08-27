package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * userId,itemId,categoryID,behavior,timestamp
  * 543462,1715,1464116,pv,1511658000
  * 156905,2901727,3001296,pv,1511658000
  * 813974,1332724,2520771,buy,1511658000
  * 524395,3887779,2366905,pv,1511658000
  * 470572,3760258,1299190,pv,1511658001
  * 543789,3110556,4558987,cart,1511658001
  * 354759,2191348,4756105,pv,1511658001
  * 723938,4719377,1464116,pv,1511658001
  */
// 输入数据的样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 中间输出的商品浏览量的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setParallelism(1)

    val dataStream: DataStream[String] = env.readTextFile("D:\\MyWork\\Scala\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "hadoop102:9092")
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("key.deserializer",
//      "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("value.deserializer",
//      "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
//    val dataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("hotitems",new SimpleStringSchema(),properties))
    //map改变数据结构
    val mapStream: DataStream[UserBehavior] = dataStream.map(line => {
      val linearray: Array[String] = line.split(",")
      UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
    })

    //配置时间戳和水位线，以备开窗函数使用eventTime(即timestamp)、watermarks
    val assignedStream: DataStream[UserBehavior] = mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(100)) {
      override def extractTimestamp(t: UserBehavior): Long = t.timestamp * 1000
    })

    val filteredStream: DataStream[UserBehavior] = assignedStream.filter(_.behavior == "pv")

    //另一种keyBy方法
    val keyedStream: KeyedStream[UserBehavior, Long] = filteredStream.keyBy(_.itemId)
    val winStream: WindowedStream[UserBehavior, Long, TimeWindow] = keyedStream.timeWindow(Time.minutes(60), Time.minutes(5))

    //每个商品在每个窗口的点击量的数据流
    val aggregatedStream: DataStream[ItemViewCount] = winStream.aggregate(new CountAgg(), new WindowResult())

    //将数据流按照窗口分类，同一个窗口的为一组
    val keyByWinStream: KeyedStream[ItemViewCount, Long] = aggregatedStream.keyBy(_.windowEnd)

    //求当前窗口中的商品点击量的TOP3
    val topNStream: DataStream[String] = keyByWinStream.process(new TopNHotItem(3))

    topNStream.print("Top N: ")

    env.execute("Hot Items Job")

  }
}

class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResult extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, aggregateResult: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId: Long = key
    val count: Long = aggregateResult.iterator.next()
    out.collect(ItemViewCount(itemId, window.getEnd, count))

  }
}

// 求某个窗口中前 N 名的热门点击商品，key 为窗口时间戳，输出为 TopN 的结果字符串
class TopNHotItem(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  // 定义一个list state，用来保存所有的ItemViewCount
  private var itemState: ListState[ItemViewCount] = _

  //初始化，在这里可以获取当前流的状态
  override def open(parameters: Configuration): Unit = {

    //命名状态变量的名字和状态变量的类型
    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state",classOf[ItemViewCount])
    //定义状态变量
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(input: ItemViewCount,
                              context: KeyedProcessFunction[Long, ItemViewCount, String]#Context,
                              collector: Collector[String]): Unit = {
    itemState.add(input)
    context.timerService().registerEventTimeTimer(input.windowEnd + 100)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    //获取收到的所有商品的点击量
    val allItems:ListBuffer[ItemViewCount] = ListBuffer()

    import scala.collection.JavaConversions._
    for (item <- itemState.get()){
      allItems.append(item)
    }

    //提前清楚状态中的数据，释放空间
    itemState.clear()
    //按点击量大小排序
    val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    //将排名信息格式化成String
    val result: StringBuffer = new StringBuffer()
    result.append("============================\n")
    result.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n")

    for(i <- sortedItems.indices){
      val currentItem: ItemViewCount = sortedItems(i)
      //e.g
      result.append("No").append(i+1).append(":")
            .append(" 商品 ID=").append(currentItem.itemId)
        .append(" 浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    // 控制输出频率，模拟实时滚动结果
//    Thread.sleep(1000)
    out.collect(result.toString)


  }
}