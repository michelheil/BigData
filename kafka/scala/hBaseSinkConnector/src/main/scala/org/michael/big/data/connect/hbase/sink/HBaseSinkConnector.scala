package org.michael.big.data.connect.hbase.sink

import java.util

import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.Task
import org.apache.kafka.connect.sink.SinkConnector
import org.slf4j.{Logger, LoggerFactory}

class HBaseSinkConnector extends SinkConnector {

  private val log: Logger = LoggerFactory.getLogger(getClass)

  val VERSION = "0.0.1"
  var configProps: util.Map[String, String] = _

  /**
   * Start this Connector. This method will only be called on a clean Connector, i.e. it has
   * either just been instantiated and initialized or {@link #stop()} has been invoked.
   *
   * @param props configuration settings
   */
  override def start(props: util.Map[String, String]): Unit = {
    configProps = props
    log.info("---------- BRIEFTAUBE ----------")
    log.info(s"Show parsed configuration in ${getClass.getName}.start: ${props}")
  }


  /**
   * Returns the Task implementation for this Connector.
   */
  override def taskClass(): Class[_ <: Task] = classOf[HBaseSinkTask]


  /**
   * Returns a set of configurations for Tasks based on the current configuration,
   * producing at most count configurations.
   *
   * @param maxTasks maximum number of configurations to generate
   * @return configurations for Tasks
   */
  override def taskConfigs(maxTasks: Int): util.List[util.Map[String, String]] = {
    val configs = new util.ArrayList[util.Map[String, String]]
    for (_ <- 0 until maxTasks) {
      configs.add(configProps)
    }
    configs
  }


  /**
   * Stop this connector.
   */
  override def stop(): Unit = {
    log.info("Stopping connector")
  }


  /**
   * Define the configuration for the connector.
   *
   * @return The ConfigDef for this connector; may not be null.
   */
  override def config(): ConfigDef = HBaseSinkConfig.config()


  /**
   * Get the version of this component.
   *
   * @return the version, formatted as a String. The version may not be (@code null} or empty.
   */
  override def version(): String = this.VERSION
}


