package config

import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Checkpoint {
  def setupEnvironment(env: StreamExecutionEnvironment, config: FlinkConfig): Unit = {
    import config._

    val checkpointConfig = env.getCheckpointConfig

    checkpointConfig.setCheckpointTimeout(checkpointTimeout)
    checkpointConfig.enableExternalizedCheckpoints(
      if (deleteExtCkptOnJobCancel) {
        ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION
      } else {
        ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
      }
    )
  }
}
