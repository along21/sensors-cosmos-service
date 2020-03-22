package services.cosmos

import com.microsoft.azure.documentdb._
import models.SensorDataChange
import play.api.Logger
import play.api.libs.json.Json

import scala.collection.JavaConverters._


object CosmosUtil {
  val log = Logger(this.getClass.getName)

  val feedOptions = {
    val options = new FeedOptions()
    options.setEnableCrossPartitionQuery(true)
    options
  }

  //Gets a database object based on the databaseID
  private def getDatabase(appCode: String, documentClient: DocumentClient): Database = {
    val databaseList = documentClient
      .queryDatabases(s"SELECT * FROM root r WHERE r.id='$appCode'", null)
      .getQueryIterable
      .toList
      .asScala
    if (databaseList.isEmpty) {
      val database: Database = new Database()
      database.setId(appCode)
      documentClient.createDatabase(database, null).getResource
    } else {
      databaseList.head
    }
  }

  //Gets a collection based on the databaseId and collectionId
  private def getCollection(appCode: String, documentClient: DocumentClient, collectionId: String): DocumentCollection = {
    val collectionList = documentClient
      .queryCollections(getDatabase(appCode, documentClient).getSelfLink, s"SELECT * FROM root r WHERE r.id='$collectionId'", null)
      .getQueryIterable
      .toList
      .asScala
    if (collectionList.isEmpty) {
      val collection: DocumentCollection = new DocumentCollection()
      collection.setId(collectionId)
      val partionKeyDefinition = new PartitionKeyDefinition()
      partionKeyDefinition.setPaths(List("/sensorUID").asJava)
      collection.setPartitionKey(partionKeyDefinition)
      documentClient.createCollection(getDatabase(appCode, documentClient).getSelfLink, collection, null).getResource
    } else {
      collectionList.head
    }
  }

  //Gets a document based on the databaseId, collectionId, and documentId
  def getDocumentBySensorAndDay(sensorUID: String, day: Int, month: Int, year: Int, appCode: String, documentClient: DocumentClient, colId: String): Document = {
    val documentList = documentClient
      .queryDocuments(getCollection(appCode, documentClient, colId).getSelfLink, s"SELECT * FROM root r WHERE r.sensorUID=\'$sensorUID\' AND r.day=$day AND r.month=$month AND r.year=$year", feedOptions)
      .getQueryIterable
      .toList
      .asScala
    documentList.isEmpty match {
      case true => null
      case _ => documentList.head
    }
  }

  def replaceDocument(document: Document, documentClient: DocumentClient) =
    documentClient
      .replaceDocument(document, new RequestOptions)
      .getStatusCode

  //Creates a document from a given model, and adds it to the named database and collection
  def createDocument(model: SensorDataChange, appCode: String, documentClient: DocumentClient, colId: String): Int = {
    val document = new Document(Json.toJson(model).toString())
    document.set("entityType", "model")
    createDocument(appCode, documentClient, colId, document)
  }

  private def createDocument(appCode: String, documentClient: DocumentClient, columnId: String, document: Document): Int = documentClient
    .createDocument(getCollection(appCode, documentClient, columnId).getSelfLink, document, null, false)
    .getStatusCode
}
