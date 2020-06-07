package org.michael.big.data.connect.hbase.sink

import org.apache.kafka.common.config.{AbstractConfig, ConfigDef}

class HBaseSinkConfig(definition: ConfigDef, originals: java.util.Map[_, _])
  extends AbstractConfig(definition, originals) {}

// contains "static" Java class members
object HBaseSinkConfig {

  val TOPIC_NAME_CONFIG = "topics"
  val TOPIC_NAME_DEFAULT = "defaultTopic"
  val TOPIC_NAME_DOC = "Name of Kafka topic(s) to source data"

  val TABLE_NAME_CONFIG = "hbase.table.name"
  val TABLE_NAME_DEFAULT = "defaultTable"
  val TABLE_NAME_DOC = "Name of HBase table to sink data"

  val COLUMN_FAMILY_CONFIG = "hbase.column.family"
  val COLUMN_FAMILY_DEFAULT = "d"
  val COLUMN_FAMILY_DOC = "Name of the HBase column family where data is written into"

  val COLUMN_QUALIFIER_CONFIG = "hbase.column.qualifier"
  val COLUMN_QUALIFIER_DEFAULT = "col"
  val COLUMN_QUALIFIER_DOC = "Name of the HBase column where data is written into"

  def config(): ConfigDef = {
    new ConfigDef()
      .define(TOPIC_NAME_CONFIG, ConfigDef.Type.STRING, TOPIC_NAME_DEFAULT, ConfigDef.Importance.HIGH, TOPIC_NAME_DOC)
      .define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, TABLE_NAME_DEFAULT, ConfigDef.Importance.HIGH, TABLE_NAME_DOC)
      .define(COLUMN_FAMILY_CONFIG, ConfigDef.Type.STRING, COLUMN_FAMILY_DEFAULT, ConfigDef.Importance.HIGH, COLUMN_FAMILY_DOC)
      .define(COLUMN_QUALIFIER_CONFIG, ConfigDef.Type.STRING, COLUMN_QUALIFIER_DEFAULT, ConfigDef.Importance.HIGH, COLUMN_QUALIFIER_DOC)
  }

}