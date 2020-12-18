import org.apache.spark.sql.streaming.{GroupState, Trigger}
import org.apache.spark.sql.{Dataset, SparkSession}

object MapGroupsWithStateExample extends App {

  case class UserAction(userId: String, action: String)
  case class UserStatus(userId: String, active: Boolean) {
    def updateWith(action: UserAction): UserStatus = {
      action match {
        case UserAction(_, x) if x == "aktiv" => UserStatus(userId, true)
        case _ =>   UserStatus(userId, false)
      }

    }
  }

  type K = String
  type V = UserAction
  type S = UserStatus
  type U = UserStatus

  def updateUserStatus(
    userId: K,
    newActions: Iterator[V],
    state: GroupState[S]): U = {


    if (state.hasTimedOut) {                // If called when timing out, remove the state
      state.remove()
    } else if (state.exists) {              // If state exists, use it for processing
      val existingState = state.get         // Get the existing state
      val shouldRemove = ???                // Decide whether to remove the state
      if (shouldRemove) {
        state.remove()                      // Remove the state
      } else {
        val newState = ???
        state.update(newState)              // Set the new state
        state.setTimeoutTimestamp(10000L,"1 hour")  // Set the timeout
      }
    } else {
      val initialState = UserStatus(userId, false)
      state.update(initialState)            // Set the initial state
      state.setTimeoutDuration("1 hour")    // Set the timeout
    }
    // return something





    val userStatus: S = state.getOption.getOrElse {
      UserStatus(userId, false)
    }

    newActions.foreach { action =>
      userStatus.updateWith(action)
    }

    state.update(userStatus)
    userStatus
  }

  val spark = SparkSession.builder()
    .appName("MapGroupsWithStateExample")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val ds: Dataset[V] = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("maxOffsetsPerTrigger", 10)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
    .as[V]

  val dsState: Dataset[U] = ds.groupByKey(userAction => userAction.userId)
    .mapGroupsWithState(updateUserStatus _)

  dsState
    .writeStream
    .format("console")
    .outputMode("update") // For queries with mapGroupsWithState, only "update" is supported as output mode (see page 363, Definitive Guide)
    .option("truncate", "false")
    .option("checkpointLocation", "/home/michael/sparkCheckpoint")
    .trigger(Trigger.ProcessingTime(1000))
    .start()
    .awaitTermination()


}


