package com.skt.tmap.rtds.function

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.typesafe.scalalogging.LazyLogging
import datatypes.{ResultStat, ResultVO}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

class SourceProcessFunction(errInputTag: OutputTag[String], statOutputTag: OutputTag[ResultStat]) extends ProcessFunction[String, ResultVO] with LazyLogging {
  val dateFormatInMillis = {
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    format.setTimeZone(TimeZone.getTimeZone("Asia/Seoul"))
    format
  }

  override def processElement(value: String, ctx: ProcessFunction[String, ResultVO]#Context, out: Collector[ResultVO]): Unit = {
    try {
      val ingestionTime = System.currentTimeMillis

      ctx.output(statOutputTag, ResultStat(ingestionTime))
      out.collect(ResultVO(ingestionTime, value))
    } catch {
      case ex: Exception =>
        ctx.output(errInputTag, ex.toString + " : " + value)
    }
  }
}
