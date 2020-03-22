package services.cosmos

import java.time.{Instant, ZoneOffset, ZonedDateTime}
import java.util.concurrent.Executors

import com.iofficecorp.appinfoclient.ApplicationInfoClient
import com.iofficecorp.metamorphosis.models.SensorEventsValueAvro
import com.microsoft.azure.documentdb.{ConnectionPolicy, ConsistencyLevel, Document, DocumentClient}
import javax.inject.Inject
import models.SensorDataEvent._
import models.{SensorDataChange, SensorDataEvent}
import org.json.JSONObject
import play.api.libs.json.Json
import play.api.{Configuration, Logger}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class CosmosUpdater @Inject()(appInfoClient: ApplicationInfoClient,
                              conf: Configuration) {

  val serviceEndpoint = conf
    .getOptional[String]("cosmos.serviceEndpoint")
    .orNull

  val documentClient = conf
    .getOptional[String]("cosmos.masterKey")
    .map(masterKey => new DocumentClient(
      serviceEndpoint,
      masterKey,
      ConnectionPolicy.GetDefault(),
      ConsistencyLevel.Session))
    .orNull

  val log = Logger(this.getClass.getName)

  private val collectionId = "SensorDataChange"

  def saveSensor(sensorData: SensorEventsValueAvro, appcode: String) = {
    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    log.debug(s"saving sensor in cosmos. ${sensorData.getSensoruid}. appcode: $appcode")
    val sensorFuture = Future {
      log.debug(s"event future started. sensor: ${sensorData.getSensoruid}, appCode: $appcode")
      val currentDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(sensorData.getEnddate), ZoneOffset.UTC)

     Option(CosmosUtil.getDocumentBySensorAndDay(sensorData.getSensoruid,
         currentDate.getDayOfMonth,
         currentDate.getMonthValue,
         currentDate.getYear,
         appcode,
         documentClient,
         collectionId)) match {
       case Some(document) => updateDocument(sensorData.getSensoruid, sensorData.getValue, appcode, currentDate.toString, document)
       case _ => createNewDocument(sensorData.getSensoruid, sensorData.getValue, sensorData.getType, appcode, currentDate)
     }
    }

    sensorFuture.onComplete({
      case Success(_) => log.debug(s"saved change event to cosmos. ${sensorData.getSensoruid}. appcode: $appcode")
      case Failure(ex) => log.error(s"Error saving change event to cosmos. ${sensorData.getSensoruid}. appcode: $appcode", ex)
    })
  }

  private def updateDocument(sensorId: String, value: Int, appcode: String, currentDate: String, document: Document) = {
    log.debug(s"adding new event for sensor: $sensorId, appCode: $appcode")
    //parse previous dates and fill in the end date for the last event. and create start of new event
    val currentEvents = Json.parse(document.getCollection("events").toString)
      .as[Seq[SensorDataEvent]]
      .map(event => event.readDateEnd match {
        case Some(_) => event
        case _ => new SensorDataEvent(event.readDateStart, Option(currentDate), event.value)
      }) :+ new SensorDataEvent(Option(currentDate.toString), None, value)

    // replace old event json list with new list in the document
    val jsonString = Json.toJson[Seq[SensorDataEvent]](currentEvents).toString()
    val jsonObject = new JSONObject(s"""{"events":$jsonString}""")

    document.set("events", jsonObject.getJSONArray("events"))
    CosmosUtil.replaceDocument(document, documentClient)
    log.debug(s"new event added for sensor: $sensorId, appCode: $appcode date:$currentDate")
  }

  private def createNewDocument(sensorId: String, value: Int, sensorType: String,  appcode: String, currentDate: ZonedDateTime) = {

    log.debug(s"creating new document for sensor: $sensorId, appCode: $appcode")

    // TODO: get the roomLink for sensor id via sensor-service api
//    implicit val executionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
//    val dbFuture = appInfoClient
//      .getDbInfoForAppCode(appcode)
//      .flatMap(dbInfo => appInfoClient
//        .getSlickDbConfig(dbInfo, Option(slickOverrideSettings))
//        .db.run(sensorRoomLinkRepository.activeBySensor(sensorId))
//      )
//    val roomLink: Seq[SensorRoomLink] = Await.result(dbFuture, Duration.apply(5, TimeUnit.MINUTES))
    val roomId = 0//if(roomLink.nonEmpty) roomLink.head.roomId else 0

    // create the previous date/value and create the current date with open ended end date, since we don't know that yet
    // TODO: grab last sensorData value from previous day
    val previousSensorData = new SensorDataEvent(Option(/*lastDate.toString*/""), Option(currentDate.toString), 0/*previousValue*/)
    val futureSensorData = new SensorDataEvent(Option(currentDate.toString), None, value)
    val sensorDataChange: SensorDataChange = new SensorDataChange(sensorId,
      currentDate.getMonthValue,
      currentDate.getDayOfMonth,
      currentDate.getYear,
      sensorType,
      roomId,
      Seq(previousSensorData, futureSensorData)
    )

    CosmosUtil.createDocument(sensorDataChange, appcode, documentClient, collectionId)
    log.debug(s"new document created for sensor: $sensorId, appCode: $appcode")
  }

  val slickOverrideSettings = Map(
    "db.maxConnections" -> 25,
    "db.numThreads" -> 25,
    "db.minConnections" -> 1,
    "db.connectionTimeout" -> "30s")
}
