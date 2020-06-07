package org.michael.big.data.connect.hbase.sink

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}
/*
class HBaseSinkConfig(definition: ConfigDef,
                      originals: java.util.Map[_, _])
  extends AbstractConfig(definition, originals) {

  val TOPIC_NAME: String = "topic"
  val TOPIC_NAME_DEFAULT: String = "defaultTopic"
  val TOPIC_NAME_DOC: String = "Name of Kafka topic to source data"
  val TABLE_NAME: String = "hbase.table.name"
  val TABLE_NAME_DEFAULT: String = "defaultTable"
  val TABLE_NAME_DOC: String = "Name of HBase table to sink dat"
  val HBASE_COLUMN_FAMILY_DEFAULT = "d"

  def config(): ConfigDef = {
    new ConfigDef()
      .define(TOPIC_NAME, ConfigDef.Type.STRING, TOPIC_NAME_DEFAULT, ConfigDef.Importance.HIGH, TOPIC_NAME_DOC)
      .define(TABLE_NAME, ConfigDef.Type.STRING, TABLE_NAME_DEFAULT, ConfigDef.Importance.HIGH, TABLE_NAME_DOC)
  }

}
*/

object HBaseSinkConfig {

  val TOPIC_NAME: String = "topics"
  val TOPIC_NAME_DEFAULT: String = "defaultTopic"
  val TOPIC_NAME_DOC: String = "Name of Kafka topic(s) to source data"
  val TABLE_NAME: String = "hbase.table.name"
  val TABLE_NAME_DEFAULT: String = "defaultTable"
  val TABLE_NAME_DOC: String = "Name of HBase table to sink dat"
  val HBASE_COLUMN_FAMILY_DEFAULT = "d"

  def config(): ConfigDef = {
    new ConfigDef()
      .define(TOPIC_NAME, ConfigDef.Type.STRING, TOPIC_NAME_DEFAULT, ConfigDef.Importance.HIGH, TOPIC_NAME_DOC)
      .define(TABLE_NAME, ConfigDef.Type.STRING, TABLE_NAME_DEFAULT, ConfigDef.Importance.HIGH, TABLE_NAME_DOC)
  }

}