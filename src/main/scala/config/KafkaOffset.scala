package config

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition

import scala.collection.JavaConverters._

object KafkaOffset {
  def kind(name: String) = name.toLowerCase
}

sealed trait KafkaOffset {
  def setupConsumer(consumer: FlinkKafkaConsumer[_], config: RuntimeConfig): Unit
}

case class Latest() extends KafkaOffset {
  override def setupConsumer(consumer: FlinkKafkaConsumer[_], config: RuntimeConfig): Unit = {
    consumer.setStartFromLatest()
  }
}
case class Ealiest() extends KafkaOffset {
  override def setupConsumer(consumer: FlinkKafkaConsumer[_], config: RuntimeConfig): Unit = {
    consumer.setStartFromEarliest()
  }
}

case class Group() extends KafkaOffset {
  override def setupConsumer(consumer: FlinkKafkaConsumer[_], config: RuntimeConfig): Unit = {
    consumer.setStartFromGroupOffsets()
  }
}

case class Timestamp(start: Long) extends KafkaOffset {
  override def setupConsumer(consumer: FlinkKafkaConsumer[_], config: RuntimeConfig): Unit = {
    consumer.setStartFromTimestamp(long2Long(start))
  }
}

case class Specific(offsets: Map[String, Long]) extends KafkaOffset {
  override def setupConsumer(consumer: FlinkKafkaConsumer[_], config: RuntimeConfig): Unit = {
    consumer.setStartFromSpecificOffsets(offsets.map {
      case _@(partition, offset) => (new KafkaTopicPartition("config.consumerTopic", partition.toInt), long2Long(offset))
    }.asJava)
  }
}