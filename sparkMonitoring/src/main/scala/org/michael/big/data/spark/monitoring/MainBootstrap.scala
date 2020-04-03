package org.michael.big.data.spark.monitoring

import com.typesafe.config.Config
import java.io.File

object MainBootstrap extends App
  with ConfigLoader {

    val fileList: List[File] = getConfigFileList("/")
    val config: Config = loadConfig(fileList)

    println(config)
    println(config.getString("auto.offset.reset"))
    println(config.getString("spark.streaming.backpressure.enabled"))
    println(config.getObject("spark.streaming").toString)

}
