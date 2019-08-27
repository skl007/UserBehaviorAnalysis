package com.atguigu.flink.network_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class ApacheLogEvent(ip:String,userId:String,eventTime:Long,method:String,url:String)

case class UrlViewCount(url:String,windowEnd:Long,count:Long)

/**
  * 实时流量统计
  * “页面浏览数”的统计，也就是读取服务器日志中的每一行log。统计每隔5秒，输出最近10分钟内访问量最多的前N个URL。
  * 66.249.73.135 - - 17/05/2015:10:05:33 +0000 GET /blog/tags/firefox?flav=rss20
  * 66.249.73.135 - - 17/05/2015:10:05:17 +0000 GET /blog/geekery/eventdb-ideas.html
  * 67.214.178.190 - - 17/05/2015:10:05:48 +0000 GET /
  * 67.214.178.190 - - 17/05/2015:10:05:18 +0000 GET /blog/geekery/installing-windows-8-consumer-preview.html
  */
object NetworkFlow {

  def main(args: Array[String]): Unit = {

    //
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //设置数据流的时间特质为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("D:\\MyWork\\Scala\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")

    val mapStream: DataStream[ApacheLogEvent] = dataStream.map(data => {
      val lineStrings: Array[String] = data.split(" ")
      val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timestamp: Long = format.parse(lineStrings(3)).getTime

      ApacheLogEvent(lineStrings(0), lineStrings(1), timestamp.toLong, lineStrings(5), lineStrings(6))
    })

    //分配时间戳和水位线
    val urlViewStream: DataStream[UrlViewCount] = mapStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime
    })
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
      .aggregate(new preAgg(), new WindowResult())


//    val value: KeyedStream[UrlViewCount, Long] = urlViewStream.keyBy(_.windowEnd)
    val processStream: DataStream[String] = urlViewStream.keyBy(_.windowEnd).process(new processResult(3))

    processStream.print("net flow top3: ")

    env.execute()

  }

}

class preAgg() extends AggregateFunction[ApacheLogEvent,Long,Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc +1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

class WindowResult() extends WindowFunction[Long,UrlViewCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {

    val urlViewCount = UrlViewCount(key,window.getEnd,input.iterator.next())
    out.collect(urlViewCount)
  }
}

class processResult(topN:Int) extends KeyedProcessFunction[Long,UrlViewCount,String]{

  private var listState:ListState[UrlViewCount] = _


  override def open(parameters: Configuration): Unit = {
    listState= getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("list-State",classOf[UrlViewCount]))
  }

  override def processElement(i: UrlViewCount,
                              context: KeyedProcessFunction[Long, UrlViewCount, String]#Context,
                              collector: Collector[String]): Unit = {

    listState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+100)
  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext,
                       out: Collector[String]): Unit = {

    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()

    val iter = listState.get().iterator()
    while( iter.hasNext )
      allUrlViews += iter.next()

    listState.clear()

    // 按照count大小排序
    val sortedUrlViews = allUrlViews.sortWith(_.count > _.count).take(topN)

    // 格式化成String打印输出
    val result: StringBuilder = new StringBuilder()
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- sortedUrlViews.indices) {
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
      result.append("No").append(i+1).append(":")
        .append("  URL=").append(currentUrlView.url)
        .append("  流量=").append(currentUrlView.count).append("\n")
    }
    result.append("====================================\n\n")

    Thread.sleep(1000)

    out.collect( result.toString() )
  }
}