
import java.time.ZoneId

import com.skt.tmap.rtds.function.{ResultStatsSink, SourceProcessFunction}
import com.typesafe.scalalogging.LazyLogging
import config._
import datatypes.ResultStat
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import sink.{CustomDateTimeBucketAssigner, CustomParquetAvroWriters}

import scala.io.Source

object StreamingFileSinkApp extends LazyLogging {

  def main(args: Array[String]): Unit = {

    val configFile = Source.fromResource("application.yaml")

    val runtimeConfig = RuntimeConfig.get(args, "RoutesTracker")
    val kafkaConfig = AppConfig.kafkaConfig()
    val flinkConfig = AppConfig.flinkConfig()
    import kafkaConfig._
    import flinkConfig._

    /* If you want to use ProtoBuffer, type the following
    *  env.getConfig.registerTypeWithKryoSerializer(classOf[T], classOf[ProtobufSerializer])
    */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.getConfig.setAutoWatermarkInterval(watermarkInterval)
    stateBackend.setupEnvironment(env)
    Checkpoint.setupEnvironment(env, flinkConfig)

    val errInputTag: OutputTag[String] = OutputTag[String]("erroneous-input")
    val routesStatOutputTag: OutputTag[ResultStat] = OutputTag[ResultStat]("routes-stat")

    /* If you want to use ProtoBuffer, type the following
    *  val consumer = new FlinkKafkaConsumer[T <: Message](consumerTopic, new ProtoBufDeserializationSchema[T <: Message](T.parseFrom(_)), consumerOptions)
    */
    val consumer = new FlinkKafkaConsumer[String](consumerTopic, new SimpleStringSchema, consumerProperty)
    flinkConfig.kafkaOffset.setupConsumer(consumer, runtimeConfig)

    val sourceStream = env.addSource(consumer)
      .setParallelism(sourceTasks)
      .name("kafka-source")
      .uid("kafka-source-id")
      .process(new SourceProcessFunction(errInputTag, routesStatOutputTag))
      .setParallelism(sourceTasks)
      .name("request-process")
      .uid("request-process-id")

    errorTopic match {
      case Some(topicName) =>
        sourceStream
          .getSideOutput(errInputTag)
          .addSink(new FlinkKafkaProducer[String](topicName, new SimpleStringSchema, errorProperty))
          .setParallelism(1)
          .name("error-to-kafka")
          .uid("error-to-kafka-id")


      case None =>
        sourceStream
          .getSideOutput(errInputTag)
          .printToErr()
          .setParallelism(1)
          .name("error-to-err")
          .uid("error-to-err-id")
    }

    if( !disableStatSinks ) {
      sourceStream
        .getSideOutput(routesStatOutputTag)
        .addSink(new ResultStatsSink(runtimeConfig))
        .setParallelism(1)
        .name("routes-stat")
        .uid("routes-stat")
    }

    val sink = StreamingFileSink
      .forBulkFormat(new Path(resultPath), CustomParquetAvroWriters.forReflectRecord())
      .withBucketAssigner(CustomDateTimeBucketAssigner("yyyyMMdd/HH", ZoneId.of("Asia/Seoul")))
      .build()

    sourceStream
      .addSink(sink)
      .setParallelism(sinkTasks)
      .name("route-sink")
      .uid("route-sink-id")

    logger.info(env.getExecutionPlan)
    env.execute()
  }
}