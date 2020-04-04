package org.michael.big.data.spark.monitoring

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

trait ConfigLoader {

  def loadConfig(appFileList: List[File]): Config = {
    appFileList match {
      case Nil => ConfigFactory.empty()
      case _ :: Nil => appFileList.map(file => ConfigFactory.parseResources(file.getName)).head
      case _ :: _ => {
        val fullList: List[Config] = appFileList.map(file => ConfigFactory.parseResources(file.getName))
        (fullList.tail :\ fullList.head)(mergeConfig)
      }
      case _ => ConfigFactory.empty()
    }
  }

  // ensure that the folder where the conf files are located are marked as project resources
  def getConfigFileList(resourcePath: String = "/"): List[File] = {
    val configFolder: File = new File(getClass.getResource(resourcePath).getPath)
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

  def mergeConfig(confA: Config, confB: Config): Config = confA.withFallback(confB)

}
