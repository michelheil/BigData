package org.michael.big.data.spark.monitoring

import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}
import org.apache.spark.sql.streaming.StreamingQueryListener.QueryProgressEvent


class MonitorListener extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = { }

  override def onQueryProgress(event: QueryProgressEvent): Unit = {
    println(s"""numInputRows: ${event.progress.numInputRows}""")
    println(s"""processedRowsPerSecond: ${event.progress.processedRowsPerSecond}""")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = { }
}


/*
{
  "id" : "a393c566-dd47-48f8-94a0-6eb9f896971a",
  "runId" : "b0757581-6f0c-4d6f-a8dc-e3e52f8c69cd",
  "name" : null,
  "timestamp" : "2020-04-14T18:24:00.000Z",
  "batchId" : 12,
  "numInputRows" : 1,
  "inputRowsPerSecond" : 0.1,
  "processedRowsPerSecond" : 1.4492753623188408,
  "durationMs" : {
    "addBatch" : 532,
    "getBatch" : 2,
    "getEndOffset" : 0,
    "queryPlanning" : 112,
    "setOffsetRange" : 4,
    "triggerExecution" : 690,
    "walCommit" : 21
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaV2[Subscribe[myInputTopic]]",
    "startOffset" : {
      "myInputTopic" : {
        "0" : 6240997
      }
    },
    "endOffset" : {
      "myInputTopic" : {
        "0" : 6240998
      }
    },
    "numInputRows" : 1,
    "inputRowsPerSecond" : 0.1,
    "processedRowsPerSecond" : 1.4492753623188408
  } ],
  "sink" : {
    "description" : "org.apache.spark.sql.kafka010.KafkaSourceProvider@79180f0c"
  }
}
 */