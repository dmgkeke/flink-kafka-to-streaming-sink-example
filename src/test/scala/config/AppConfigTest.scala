package config

import org.scalatest.{FlatSpec, Outcome}
import pureconfig.{ConfigSource, _}

class AppConfigTest extends FlatSpec {
  import pureconfig.generic.auto._

  val defaultSource = ConfigSource.default
  val newSource = ConfigSource.resources("new.conf")

  override protected def withFixture(test: NoArgTest): Outcome = {
    assertDefaultSource

    try {
      super.withFixture(test)
    }finally {
      assertDefaultSource
    }
  }

  def assertDefaultSource = {
    assertResult(defaultSource) {
      AppConfig.defaultSource
    }
  }

  "AppConfig" should "load config from default resource directory" in {
    val kafkaConfig = AppConfig.kafkaConfig()
    val flinkConfig = AppConfig.flinkConfig()

    assertResult(defaultSource.at(AppConfig.kafkaNamespace).loadOrThrow[KafkaConfig]) {
      kafkaConfig
    }

    assertResult(defaultSource.at(AppConfig.flinkNamespace).loadOrThrow[FlinkConfig]) {
      flinkConfig
    }
  }

  "AppConfig" should "overwrite config on merged config source" in {
    val mergedConfig = AppConfig.merge(newSource)
    val kafkaConfig = AppConfig.kafkaConfig(mergedConfig)
    val flinkConfig = AppConfig.flinkConfig(mergedConfig)

    assertResult(defaultSource.at(AppConfig.kafkaNamespace).loadOrThrow[KafkaConfig]) {
      kafkaConfig
    }

    val defaultFlinkConfig = defaultSource.at(AppConfig.flinkNamespace).loadOrThrow[FlinkConfig]
    assert(flinkConfig != defaultFlinkConfig)

    assertResult(flinkConfig) {
      defaultFlinkConfig.copy(stateBackend = flinkConfig.stateBackend, kafkaOffset = flinkConfig.kafkaOffset)
    }
  }

  "AppConfig" should "same config source with same file" in {
    assertResult(AppConfig.merge("src/test/resources/new.conf").config()) {
      AppConfig.merge(newSource).config()
    }
  }
}
