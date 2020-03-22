package services.kafka

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.Executors

import com.google.inject.AbstractModule
import com.iofficecorp.appinfoclient.ApplicationInfoClient
import com.iofficecorp.metamorphosis.IofficeKafka
import com.iofficecorp.metamorphosis.IofficeKafka.IofficeStreams._
import com.iofficecorp.metamorphosis.IofficeKafka.{brokers, streamsConfiguration}
import com.iofficecorp.metamorphosis.models.{SensorEventsKeyAvro, SensorEventsValueAvro}
import com.lightbend.kafka.scala.streams.ImplicitConversions._
import com.lightbend.kafka.scala.streams.StreamsBuilderS
import javax.inject.Inject
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import play.api.Configuration
import services.cosmos.CosmosUpdater

import scala.concurrent.ExecutionContext


class SensorDataSubscriber @Inject()(conf: Configuration,
                                 appInfoClient: ApplicationInfoClient,
                               ) extends KafkaSubscriber {

  implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())

  private val saveSensors = conf.getOptional[String]("kafka.saveSensors").getOrElse("false").equals("true")

  override def isEnvironmentVarsConfigured: Boolean = conf
    .getOptional[String]("kafka.brokers")
    .isDefined &&
    conf
      .getOptional[String]("kafka.schemaRegistryUrl")
      .isDefined

  override def isSubscriberConfOn: Boolean = conf
    .getOptional[String]("kafka.runSubscriber")
    .getOrElse("true")
    .equals("true")

  override def subscribedTopicName: String = conf
    .getOptional[String]("kafka.topicName")
    .getOrElse("sensor-events")

  override def getTaskId: String = conf
    .getOptional[String]("kafka.subscriberName")
    .getOrElse("sensor-service-subscriber-SensorTask")

  private val postToCosmos = conf.getOptional[String]("cosmos.masterKey").isDefined &&
    conf.getOptional[String]("cosmos.serviceEndpoint").isDefined

  override def startSubscriber(brokers: String, taskId: String): KafkaStreams = StartDataSubscriberStream(readSensorEvent, IofficeKafka.brokers, taskId, subscribedTopicName)

  override def logSensorProccessing(sensorData: SensorEventsValueAvro, appCode: String): Unit =
    log.warn(s"endDate=${ZonedDateTime.ofInstant(Instant.ofEpochMilli(sensorData.getEnddate), ZoneOffset.UTC)}. now=${Instant.now().toString}. appcode=$appCode")

  var cosmosUpdater: Option[CosmosUpdater] = None

  def readSensorEvent(key: String, value: SensorEventsValueAvro, retryCount: Int = 0): Unit = {

    if (postToCosmos) {
      // dont create this class unless 'postToCosmos' is true to ensure we have the conf val cosmos.masterKey
      cosmosUpdater.getOrElse({
        cosmosUpdater = Option(new CosmosUpdater(appInfoClient, conf))
        cosmosUpdater.get
      }).saveSensor(value, key)
    }

    logTheThing(value, key)
  }

  object StartDataSubscriberStream {

    def apply(processSensor: (String, SensorEventsValueAvro, Int) => Unit,
              brokers: String = brokers,
              instanceId: String,
              subscribedTopicName: String): KafkaStreams = {
      val streamsConf = streamsConfiguration(instanceId, brokers)
      streamsConf.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "30000")
      streamsConf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
      streamsConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")
      streamsConf.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, classOf[LogAndContinueExceptionHandler])

      val builder = new StreamsBuilderS()

      builder
        .stream[SensorEventsKeyAvro, SensorEventsValueAvro](subscribedTopicName)
        .foreach((key: SensorEventsKeyAvro, value: SensorEventsValueAvro) => {
          processSensor(key.getAppCode, value, 0)
          ()
        })

      buildStream(builder, streamsConf)
    }
  }

}

class DataSubscriber @Inject()(subscriber: SensorDataSubscriber) {
  subscriber.startSensorSubscriberStream()
}

class SensorDataSubscriberModule extends AbstractModule {
  override def configure() = {
    bind(classOf[DataSubscriber]).asEagerSingleton()
  }
}