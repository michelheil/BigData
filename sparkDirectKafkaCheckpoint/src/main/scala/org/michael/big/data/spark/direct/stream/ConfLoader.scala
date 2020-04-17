package org.michael.big.data.spark.direct.stream

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

trait ConfLoader {

  val conf: Config

  def loadConfigFromPath(resourceFilePath: String): Config = {

    // The composition 'andThen' is defined for functions.
    // Therefore, it is required to define pathToFileList as a function literal
    // and not as a usual method (using 'def').
    // The method 'fileListToConfig' can still be defined as 'def' as the
    // compiler will automatically convert the def into a function.
    // The conversion can explicitly be done through 'filesToConfig _'.
    // See https://stackoverflow.com/questions/7505304/compose-and-andthen-methods
    (pathToFileList andThen fileListToConfig)(resourceFilePath)
  }

  def fileListToConfig(appFileList: List[File]): Config = appFileList match {
      case Nil        => ConfigFactory.empty()
      case _ :: Nil   => appFileList.map(file => ConfigFactory.parseResources(file.getName)).head
      case head :: tl => {
        val fullList: List[Config] = appFileList.map(file => ConfigFactory.parseResources(file.getName))
        (fullList.tail :\ fullList.head)(mergeConfig)
      }
      case _          => ConfigFactory.empty()
    }

  // define as 'function1' literal so we can compose it with other functions using 'andThen'.
  val pathToFileList = (resourceFilePath: String) => {
    val configFolder: File = new File(resourceFilePath)
    if (configFolder.exists && configFolder.isDirectory) {
      val configFiles: List[File] = configFolder
        .listFiles
        .toList
        .filter(_.getName.contains("conf"))

      configFiles
    } else {
      List()
    }
  }

  private def mergeConfig(confA: Config, confB: Config): Config = confA.withFallback(confB)

}


