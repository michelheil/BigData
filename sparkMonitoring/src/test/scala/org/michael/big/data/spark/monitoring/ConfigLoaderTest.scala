package org.michael.big.data.spark.monitoring

import java.io.File

import com.typesafe.config.Config
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class ConfigLoaderTest extends AnyFeatureSpec
  with GivenWhenThen
  with ConfigLoader {

  Feature("Configuration loading") {
    Scenario("Load all conf files that are available in Test Resources") {
      val fileList: List[File] = getConfigFileList("/")
      val config: Config = loadConfig(fileList)

      println(config)
      println(config.getString("auto.offset.reset"))

      val result: String = config.getString("auto.offset.reset")

      assertResult("earliest")(result)
    }
  }

  Feature("Config merge operation") {
    Scenario("merge two different configuration") {
      pending
    }

    Scenario("merge two identical configuration") {
      pending
    }

    Scenario("merge two empty configuration") {
      pending
    }

    Scenario("merge one non-empty with one empty configuration") {
      pending
    }
  }

  Feature("Config merge operation") {
    Scenario("merge two different configuration") {
      pending
    }

    Scenario("merge two identical configuration") {
      pending
    }

    Scenario("merge two empty configuration") {
      pending
    }

    Scenario("merge one non-empty with one empty configuration") {
      pending
    }
  }


}
