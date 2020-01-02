package config

import java.util.Properties

import pureconfig.generic.FieldCoproductHint
import pureconfig.{ConfigObjectSource, ConfigReader, ConfigSource}
import pureconfig.generic.auto._

object AppConfig {
  lazy val defaultSource = ConfigSource.default

  private[config] val rootNamespace = "app-config"
  private[config] val kafkaNamespace = s"$rootNamespace.kafka"
  private[config] val flinkNamespace = s"$rootNamespace.flink"

  private implicit val stateBackendHint = new FieldCoproductHint[StateBackendEnv]("kind") {
    override protected def fieldValue(name: String): String = {
      StateBackendEnv.kind(name)
    }
  }

  private implicit val kafkaOffsetHint = new FieldCoproductHint[KafkaOffset]("kind") {
    override protected def fieldValue(name: String): String = {
      KafkaOffset.kind(name)
    }
  }

  def merge(confPath:String) = {
    defaultSource.withFallback(ConfigSource.file(confPath).optional)
  }

  def merge(source:ConfigObjectSource) = {
    defaultSource.withFallback(source.optional)
  }

  def kafkaConfig(configSource:ConfigObjectSource = defaultSource): KafkaConfig = configSource.at(kafkaNamespace).load[KafkaConfig] match {
    case Left(failures) => {
      println(failures)
      defaultSource.at(kafkaNamespace).loadOrThrow[KafkaConfig]
    }
    case Right(value) => value
  }

  def flinkConfig(configSource:ConfigObjectSource = defaultSource): FlinkConfig = configSource.at(flinkNamespace).load[FlinkConfig] match {
    case Left(failures) => {
      println(failures)
      defaultSource.at(flinkNamespace).loadOrThrow[FlinkConfig]
    }
    case Right(value:FlinkConfig) => value
  }
}

case class KafkaConfig(
                        bootstrapServers: Seq[String] = Seq.empty,
                        consumerTopic: String,
                        consumerGroupId: String,
                        consumerOptions: Map[String, Either[String, Int]] = Map.empty,
                        errorTopic: Option[String] = None,
                        errorTopicOptions: Map[String, Either[String, Int]] = Map.empty
                ) {

  def consumerProperty = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers.mkString(","))
    properties.setProperty("group.id", consumerGroupId)
    consumerOptions.foreach(option => {
      properties.setProperty(option._1, option._2 match {
        case Right(x) => x.toString
        case Left(x) => x
      })
    })

    properties
  }

  def errorProperty = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", bootstrapServers.mkString(","))
    errorTopicOptions.foreach(option => {
      properties.setProperty(option._1, option._2 match {
        case Right(x) => x.toString
        case Left(x) => x
      })
    })

    properties
  }

}

case class FlinkConfig(
                        sourceTasks: Int = 1,
                        sinkTasks: Int = 1,
                        resultPath: String = "",
                        watermarkInterval: Long = 50L,
                        maxOutOfOrderness: Long = 0L,
                        histogramReservoirMillis: Long = 1000L,
                        checkpointTimeout: Long = 600000L,
                        stateBackend: StateBackendEnv = NoneStateBackendEnv(),
                        kafkaOffset: KafkaOffset = Latest(),
                        deleteExtCkptOnJobCancel: Boolean = false,
                        disableStatSinks: Boolean = false
                      )