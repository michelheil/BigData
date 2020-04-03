package org.michael.big.data.spark.monitoring

import java.io.File
import com.typesafe.config.{Config, ConfigFactory}

trait ConfigLoader {

  def loadConfig(appFileList: List[File]): Config = {
    val xs: List[Config] = appFileList.map(file => ConfigFactory.parseResources(file.getName))
    val merged = (xs.tail :\ xs.head)(op)
    merged
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

  def op(confA: Config, confB: Config): Config = confA.withFallback(confB)

}
