import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskEnd
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// register:
// val spillTaskListener = new SpillTaskListener()
// spark.sparkContext.addSparkListener(spillTaskListener)
//
// apply in code:
// spillTaskListener.reset()
// [...] custom code
// spillTaskListener.report()
class SpillTaskListener extends SparkListener {
  val stageIdToTaskMetrics = new mutable.HashMap[Int, ArrayBuffer[TaskMetrics]]
  val spilledStageIds = new mutable.HashSet[Int]

  def numSpilledStages: Int = synchronized {
    spilledStageIds.size
  }

  def reset(): Unit = synchronized {
    spilledStageIds.clear()
    stageIdToTaskMetrics.clear()
  }

  def report(): Unit = synchronized {

    stageIdToTaskMetrics.foreach(x => {
      val (stageId, metrics): (Int, ArrayBuffer[TaskMetrics]) = x
      println("StageId: " + stageId)
      metrics.zipWithIndex.foreach(metric => {
        if (metric._1.memoryBytesSpilled > 0) {
          println("Task: " + metric._2 + "; MemoryBytesSpilled: " + metric._1.memoryBytesSpilled)
        }
      })
    })

  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    stageIdToTaskMetrics.getOrElseUpdate(
      taskEnd.stageId, new ArrayBuffer[TaskMetrics]) += taskEnd.taskMetrics
  }

}
