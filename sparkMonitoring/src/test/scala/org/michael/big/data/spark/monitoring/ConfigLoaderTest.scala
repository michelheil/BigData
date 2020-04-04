package org.michael.big.data.spark.monitoring

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class ConfigLoaderTest extends AnyFeatureSpec
  with GivenWhenThen
  with ConfigLoader {

  Feature("Test ConfigLoader") {
    Scenario("Load all conf files that are available in Test Resources") {
      val fileList: List[File] = getConfigFileList("/")
      val config: Config = loadConfig(fileList)

      val result: String = config.getString("auto.offset.reset")

      assertResult("earliest")(result)
    }
  }

  Feature("Test getConfigFileList") {
    Scenario("No Path is given") {
      pending
    }

    Scenario("""Path is '/'""") {
      pending
    }

    Scenario("""Path is 'test/'""") {
      pending
    }

    Scenario("""Path is '/test/'""") {
      pending
    }

    Scenario("""Path is 'anyString'""") {
      pending
    }
  }

  // def loadConfig(appFileList: List[File]): Config
  Feature("Test loadConfig") {
    Scenario("List with 0 elements") {
      Given("an empty list")
      val emptyList: List[File] = List()
      assertResult(true)(emptyList.isEmpty)

      When("applying method")
      val resultConfig: Config = loadConfig(emptyList)

      Then("resulting Config should be empty")
      val emptyConfig: Config = ConfigFactory.empty()
      assertResult(emptyConfig)(resultConfig)
    }

    Scenario("List with 1 element") {
      Given("a list with one File")
      val fileOne = new File(getClass.getResource("/kafkaTest.conf").getPath)
      val givenList: List[File] = List[File](fileOne)

      When("applying method")
      val resultConfig = loadConfig(givenList)

      Then("resulting Config should contain the configuration of the one File")
      val result: String = resultConfig.getString("auto.offset.reset")
      assertResult("earliest")(result)
    }

    Scenario("List with 2 element") {
      Given("a list with two File")
      val fileOne = new File(getClass.getResource("/kafkaTest.conf").getPath)
      val fileTwo = new File(getClass.getResource("/sparkTest.conf").getPath)
      val givenList: List[File] = List[File](fileOne, fileTwo)

      When("applying method")
      val resultConfig = loadConfig(givenList)
      println(resultConfig)

      Then("resulting Config should contain the configuration of the two File")
      val result1: String = resultConfig.getString("auto.offset.reset")
      assertResult("earliest")(result1)
      val result2: Boolean = resultConfig.getBoolean("spark.streaming.backpressure.enabled")
      assertResult(true)(result2)
    }
  }


}
