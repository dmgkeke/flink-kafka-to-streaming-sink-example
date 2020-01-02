package config

import java.io.File
import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import scopt.OptionParser

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class RuntimeConfig(
                         histogramReservoirMillis: Long = 1000L,
                         checkpointTimeout: Long = 600000L,
//                         kafkaOffset: KafkaOffset = LATEST(),
                         externalizedCheckpoint: Boolean = false,
                         deleteExtCkptOnJobCancel: Boolean = false,
                         incrementalCheckpoint: Boolean = false,
                         disableStatSinks: Boolean = false,
                         resultPath: String = ""
                        )

object RuntimeConfig extends LazyLogging {
  implicit val propertiesRead: scopt.Read[Properties] =
    scopt.Read.reads { x =>
      val properties = new Properties()
      (properties: java.util.Map[Object, Object]).putAll(
        implicitly[scopt.Read[Map[String,String]]].reads(x).asJava
      )
      properties
    }

  def get(args: Array[String], programName: String): RuntimeConfig = {
    val parser = new OptionParser[RuntimeConfig](programName) {
      override def reportError(msg: String): Unit = logger.error(msg)
      override def reportWarning(msg: String): Unit = logger.warn(msg)
      head(programName)

      help("help").text("prints this usage text")

      opt[Long]("histogram-reservoir-ms")
        .action((x, c) => c.copy(histogramReservoirMillis = x))
        .valueName("<ms>")
        .text("default : 1000")

      opt[Long]("checkpoint-timeout")
        .action((x, c) => c.copy(checkpointTimeout = x))
        .valueName("<ms>")
        .text("default : 600000")

      opt[Unit]("externalized-checkpoint")
        .action((x, c) => c.copy(externalizedCheckpoint = true))

      opt[Unit]("externalized-checkpoint-deletion-on-job-cancel")
        .action((x, c) => c.copy(deleteExtCkptOnJobCancel = true))

      opt[Unit]("incremental-checkpoint")
        .action((x, c) => c.copy(incrementalCheckpoint = true))
        .text("effective only when using a rocksdb state-backend")

      opt[Unit]("disable-stat-sinks")
        .action((x, c) => c.copy(disableStatSinks = true))

      opt[String]("result-path")
        .required()
        .action((x, c) => c.copy(resultPath = x))
    }

    parser.parse(args, RuntimeConfig()) match {
      case Some(config) => config
      case None => throw new RuntimeException("Failed to get a valid configuration object")
    }
  }
}