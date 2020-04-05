package org.michael.big.data.spark.monitoring

object MainBootstrap extends App
  with ConfigLoader {

    // ensure that the folder where the conf files are located are marked as project resources
    val config = loadConfigFromPath(getClass.getResource("/").getPath)

    println(config)
    println(config.getString("auto.offset.reset"))
    println(config.getString("spark.streaming.backpressure.enabled"))
    println(config.getObject("spark.streaming").toString)

}
