package config

import com.typesafe.scalalogging.LazyLogging
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object StateBackendEnv {
  def kind(name: String) = name.dropRight("StateBackendEnv".length).toLowerCase
}

sealed trait StateBackendEnv extends LazyLogging {
  val intervalInMillis: Long = 0
  val pauseInMillis: Option[Long] = None

  def setupEnvironment(env: StreamExecutionEnvironment) = {
    flinkStateBackend() match {
      case Some(stateBackend) => {
        env.setStateBackend(stateBackend)
        env.enableCheckpointing(intervalInMillis)
        pauseInMillis match {
          case Some(pause) => env.getCheckpointConfig.setMinPauseBetweenCheckpoints(pause)
          case None =>
        }
      }
      case None => logger.warn("Because state backend is not specified, data may be lost.")
    }
  }

  def flinkStateBackend(): Option[StateBackend]

  override def toString: String = {
    s"${this.getClass.getSimpleName} : {intervalInMillis : $intervalInMillis, pauseInMillis: $pauseInMillis}"
  }
}

case class NoneStateBackendEnv() extends StateBackendEnv {
  override def flinkStateBackend(): Option[StateBackend] = None
}

case class MemoryStateBackendEnv() extends StateBackendEnv {
  override def flinkStateBackend(): Option[StateBackend] = {
    Some(new MemoryStateBackend())
  }
}

case class FsStateBackendEnv(uri: String) extends StateBackendEnv {
  override def flinkStateBackend(): Option[StateBackend] = {
    Some(new FsStateBackend(uri))
  }
}

case class RocksDBStateBackendEnv(uri: String, incrementalCheckpoint: Boolean = false, ssdOptimization: Boolean = false) extends StateBackendEnv {
  override def flinkStateBackend(): Option[StateBackend] = {
    val rocksDBStateBackend = new RocksDBStateBackend(uri, incrementalCheckpoint)
    if (ssdOptimization) {
      rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.FLASH_SSD_OPTIMIZED)
    } else {
      rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED)
    }

    Some(rocksDBStateBackend)
  }
}