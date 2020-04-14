package org.michael.big.data.spark.monitoring

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec

class ConfLoaderTest extends AnyFeatureSpec
  with GivenWhenThen
  with ConfLoader {

  Feature("Test loadConfigFromPath") {
    Scenario("Load all conf files that are available in Test Resources") {
      Given("a path where the configuration files are located")
      val configPath = getClass.getResource("/").getPath

      When("applying the method")
      val config = loadConfigFromPath(configPath)

      Then("the content of the files are loaded and can be used")
      val result: String = config.getString("auto.offset.reset")
      assertResult("earliest")(result)
    }
  }

  Feature("Test pathToFileList") {
    Scenario("""Relative path is '/'""") {
      Given("a path where the configuration files are located")
      val configPath = getClass.getResource("/").getPath

      When("applying the method")
      val fileList = pathToFileList(configPath)
      val fileNameList = fileList.map(fileFullPath => fileFullPath.getName)

      Then("the conf files in the path are listed")
      val containsFile1 = fileNameList.contains("sparkTest.conf")
      val containsFile2 = fileNameList.contains("kafkaTest.conf")
      val doesNotContainFile = fileNameList.contains("hbaseTest.conf")
      assertResult(true)(containsFile1)
      assertResult(true)(containsFile2)
      assertResult(false)(doesNotContainFile)
    }

    Scenario("""Relative path does not exist""") {
      Given("a path that does not exists")
      val configPath = "/notExist"

      When("applying the method")
      val fileList = pathToFileList(configPath)

      Then("the conf files in the path are listed")
      assertResult(true)(fileList.isEmpty)
    }

    Scenario("""Relative path is not a directory""") {
      Given("a path that is not a directory")
      val configPath = "1"

      When("applying the method")
      val fileList = pathToFileList(configPath)

      Then("the conf files in the path are listed")
      assertResult(true)(fileList.isEmpty)
    }
  }

  Feature("Test fileListToConfig") {
    Scenario("List with 0 elements") {
      Given("an empty list")
      val emptyList: List[File] = List()
      assertResult(true)(emptyList.isEmpty)

      When("applying method")
      val resultConfig: Config = fileListToConfig(emptyList)

      Then("resulting Config should be empty")
      val emptyConfig: Config = ConfigFactory.empty()
      assertResult(emptyConfig)(resultConfig)
    }

    Scenario("List with 1 element") {
      Given("a list with one File")
      val fileOne = new File(getClass.getResource("/kafkaTest.conf").getPath)
      val givenList: List[File] = List[File](fileOne)

      When("applying method")
      val resultConfig = fileListToConfig(givenList)

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
      val resultConfig = fileListToConfig(givenList)

      Then("resulting Config should contain the configuration of the two File")
      val result1: String = resultConfig.getString("auto.offset.reset")
      assertResult("earliest")(result1)
      val result2: Boolean = resultConfig.getBoolean("spark.streaming.backpressure.enabled")
      assertResult(true)(result2)
    }
  }


}
