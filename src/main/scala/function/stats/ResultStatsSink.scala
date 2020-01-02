package com.skt.tmap.rtds.function

import java.util.concurrent.TimeUnit

import com.codahale.metrics.SlidingTimeWindowArrayReservoir
import config.RuntimeConfig
import datatypes.ResultStat
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.{DropwizardHistogramWrapper, DropwizardMeterWrapper}
import org.apache.flink.metrics.{Histogram, Meter}
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class ResultStatsSink(runtimeConfig: RuntimeConfig) extends RichSinkFunction[ResultStat] {
  @transient private var routesMeter: Meter = _
  @transient private var sourceProcessingTimeHistogram: Histogram = _

  override def open(parameters: Configuration): Unit = {
    routesMeter = getRuntimeContext()
      .getMetricGroup()
      .meter("routesMeter", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()))

    sourceProcessingTimeHistogram = getRuntimeContext()
      .getMetricGroup()
      .histogram("sourceProcessingTimeHistogram", histogramWrapper)
  }

  def histogramWrapper() = new DropwizardHistogramWrapper(
    new com.codahale.metrics.Histogram(
      new SlidingTimeWindowArrayReservoir(runtimeConfig.histogramReservoirMillis, TimeUnit.MILLISECONDS)
    )
  )

  override def invoke(value: ResultStat, context: SinkFunction.Context[_]): Unit = {
    routesMeter.markEvent()
    sourceProcessingTimeHistogram.update(System.currentTimeMillis - value.ingestionTime)
  }
}
