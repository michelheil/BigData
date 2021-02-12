package org.michael.big.data.spark

import org.apache.kafka.clients.consumer.ConsumerRecord

object Processor extends Serializable {
  def extractValue(it: Iterator[ConsumerRecord[String, String]]): Iterator[String] = {
    it.map(cr => {
      cr.value()
    })
  }
}
