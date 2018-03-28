
package org.apache.flink.table.eventtime

import java.util.concurrent.TimeUnit

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class LinearTimestamp extends AssignerWithPunctuatedWatermarks[String] {
  var counter = 0L

  override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
    counter += 1000L
    counter
  }

  override def checkAndGetNextWatermark(lastElement: String, extractedTimestamp: Long) = {
    new Watermark(counter - 1)
  }
}
/*class LinearTimestamp extends AssignerWithPeriodicWatermarks[String] with Serializable{
  /*override def extractTimestamp(e: String, prevElementTimestamp: Long) = {
  e.split(",")(1).
}*/
  override def getCurrentWatermark(): Watermark = {
  new Watermark(System.currentTimeMillis)
}
}*/