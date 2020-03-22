package services.kafka

import com.iofficecorp.metamorphosis.IofficeKafka
import com.iofficecorp.metamorphosis.models.SensorEventsValueAvro
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KafkaStreams.State
import org.apache.kafka.streams.processor.StateRestoreListener
import play.api.Logger

import scala.util.{Failure, Success, Try}

trait KafkaSubscriber {

  var resetStreamSetup = false
  var logCount = -1
  val log = Logger(this.getClass.getName)

  def logSensorProccessing(sensorData: SensorEventsValueAvro, appCode: String)

  def isEnvironmentVarsConfigured : Boolean

  def isSubscriberConfOn: Boolean

  def getTaskId: String

  def startSubscriber(brokers: String, taskId: String): KafkaStreams

  def subscribedTopicName: String

  val slickOverrideSettings = Map(
    "db.maxConnections" -> 25,
    "db.numThreads" -> 25,
    "db.minConnections" -> 1,
    "db.connectionTimeout" -> "30s")

  def startSensorSubscriberStream(): Unit = if (!resetStreamSetup) {
    resetStreamSetup = true
    if (isEnvironmentVarsConfigured && isSubscriberConfOn) {
      val stream = startSubscriber(IofficeKafka.brokers, getTaskId)
      stream.setGlobalStateRestoreListener(new RestoreListener)
      stream.setStateListener(new StateListener)
      stream.setUncaughtExceptionHandler(new ExceptionHandler)
      if (stream.state != State.RUNNING) {
        Try({
          stream.cleanUp
          stream.start
        }) match {
          case Success(_) => ()
          case Failure(ex) =>
            log.error(s"the start failed: ${ex.getMessage}")
            Try(stream.close) match {
              case Success(_) => ()
              case Failure(ex2) => {
                log.error(s"The stop failed ${ex2.getMessage}")
              }
            }
        }
      }
      sys.addShutdownHook({
        stream.close()
      })
      ()
    } else {
      log.error(s"It looks like you really want to start the topic stream here but the env vars don't exist!")
    }
  }

  class ExceptionHandler extends Thread.UncaughtExceptionHandler {
    val log = Logger(this.getClass.getName)
    var stream: KafkaStreams = _

    override def uncaughtException(t: Thread, e: Throwable): Unit = {
      log.error(s"Uncaught Exception! Thread: ${t.toString}.  ${e.toString}")
      resetStreamSetup = false
      startSensorSubscriberStream
    }
  }

  // These two listener were added for logging purposes
  class RestoreListener extends StateRestoreListener {
    val log = Logger(this.getClass.getName)

    override def onRestoreStart(topicPartition: TopicPartition, storeName: String, startingOffset: Long, endingOffset: Long): Unit = {
      log.warn(s"onRestoreStart Called!: TopicPartition: ${topicPartition.toString}. storeName: $storeName. startingOffset: $startingOffset. endingOffset:$endingOffset")
    }

    override def onBatchRestored(topicPartition: TopicPartition, storeName: String, batchEndOffset: Long, numRestored: Long): Unit = {
      log.debug(s"onBatchRestored-TopicPartition: ${topicPartition.toString}. storeName: $storeName. batchEndOffset: $batchEndOffset. numRestored:$numRestored")
    }

    override def onRestoreEnd(topicPartition: TopicPartition, storeName: String, totalRestored: Long): Unit = {
      log.warn(s"onRestoreEnd Called!: TopicPartition: ${topicPartition.toString}. storeName: $storeName. totalRestored: $totalRestored.")
    }
  }

  class StateListener extends KafkaStreams.StateListener {
    val log = Logger(this.getClass.getName)
    override def onChange(newState: State, oldState: State): Unit = {
      log.warn(s"StateListener onChange Called!: newState: ${newState.toString}. oldState: ${oldState.toString} ")
    }
  }

  def logTheThing(sensorData: SensorEventsValueAvro, appCode: String) = {
    // avoid logging every time. slows down whole thing and it starts to lag farther and farther behind.
    if (logCount > 100 || logCount == -1) {
      logCount = 0
      logSensorProccessing(sensorData, appCode)
    }
    logCount = logCount + 1
  }
}
