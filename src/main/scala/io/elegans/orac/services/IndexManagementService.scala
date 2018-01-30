package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import java.io._

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings._
import org.elasticsearch.common.xcontent.XContentType

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source


/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object IndexManagementService {
  val elasticClient: IndexManagementClient.type = IndexManagementClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val schemaFiles: List[JsonIndexFiles] = List[JsonIndexFiles](
    JsonIndexFiles(path = "/index_management/json_index_spec/general/action.json",
      updatePath = "/index_management/json_index_spec/general/update/action.json",
      indexSuffix = elasticClient.actionIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/general/item.json",
      updatePath = "/index_management/json_index_spec/general/update/item.json",
      indexSuffix = elasticClient.itemIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/general/item_info.json",
      updatePath = "/index_management/json_index_spec/general/update/item_info.json",
      indexSuffix = elasticClient.itemInfoIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/general/orac_user.json",
      updatePath = "/index_management/json_index_spec/general/update/orac_user.json",
      indexSuffix = elasticClient.userIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/general/recommendation.json",
      updatePath = "/index_management/json_index_spec/general/update/recommendation.json",
      indexSuffix = elasticClient.recommendationIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/general/recommendation_history.json",
      updatePath = "/index_management/json_index_spec/general/update/recommendation_history.json",
      indexSuffix = elasticClient.recommendationHistoryIndexSuffix)
  )

  def createIndex(indexName: String, language: String) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val analyzerJsonPath: String = "/index_management/json_index_spec/" + language + "/analyzer.json"
    val analyzerJsonIs: Option[InputStream] = Option{getClass.getResourceAsStream(analyzerJsonPath)}
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "Check the file: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.map(item => {

      val jsonInStream: Option[InputStream] = Option{getClass.getResourceAsStream(item.path)}

      val schemaJson: String = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.path + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = indexName + "." + item.indexSuffix

      val createIndexRes: CreateIndexResponse =
        client.admin().indices().prepareCreate(fullIndexName)
          .setSettings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON))
          .setSource(schemaJson, XContentType.JSON).get()

      item.indexSuffix + "(" + fullIndexName + ", " + createIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexCreation: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def removeIndex(indexName: String) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    if (! elasticClient.enableDeleteIndex) {
      val message: String = "operation is not allowed, contact system administrator"
      throw new Exception(message)
    }

    val operationsMessage: List[String] = schemaFiles.map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix

      val deleteIndexRes: DeleteIndexResponse =
        client.admin().indices().prepareDelete(fullIndexName).get()

      item.indexSuffix + "(" + fullIndexName + ", " + deleteIndexRes.isAcknowledged.toString + ")"

    })

    val message = "IndexDeletion: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def checkIndex(indexName: String) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val operationsMessage: List[String] = schemaFiles.map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val getMappingsReq = client.admin.indices.prepareGetMappings(fullIndexName).get()
      val check = getMappingsReq.mappings.containsKey(fullIndexName)
      item.indexSuffix + "(" + fullIndexName + ", " + check + ")"
    })

    val message = "IndexCheck: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def checkIndexStatus(indexName: String) : Boolean = {
    val client: TransportClient = elasticClient.getClient

    val operationsMessage: List[Boolean] = schemaFiles.map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val check = try {
        val getMappingsReq = client.admin.indices.prepareGetMappings(fullIndexName).get()
        getMappingsReq.mappings.containsKey(fullIndexName)
      } catch {
        case e: Exception =>
          false
      }
      check
    })
    val status = operationsMessage.reduce((x, y) => x && y)
    status
  }

  def updateIndex(indexName: String, language: String) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val analyzerJsonPath: String = "/index_management/json_index_spec/" + language + "/analyzer.json"
    val analyzerJsonIs: Option[InputStream] = Option{getClass.getResourceAsStream(analyzerJsonPath)}
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "Check the file: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.map(item => {
      val jsonInStream: Option[InputStream] = Option{getClass.getResourceAsStream(item.updatePath)}

      val schemaJson: String = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.path + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = indexName + "." + item.indexSuffix

      val updateIndexRes  =
        client.admin().indices().preparePutMapping().setIndices(fullIndexName)
          .setType(item.indexSuffix)
          .setSource(schemaJson, XContentType.JSON).get()

      item.indexSuffix + "(" + fullIndexName + ", " + updateIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexUpdate: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def refreshIndexes(indexName: String) : Future[Option[RefreshIndexResults]] = Future {
    val client: TransportClient = elasticClient.getClient

    val operationsResults: List[RefreshIndexResult] = schemaFiles.map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val refreshIndexRes: RefreshIndexResult = elasticClient.refreshIndex(fullIndexName)
      if (refreshIndexRes.failed_shards_n > 0) {
        val indexRefreshMessage = item.indexSuffix +
          "(" + fullIndexName + ", " + refreshIndexRes.failed_shards_n + ")"
        throw new Exception(indexRefreshMessage)
      }

      refreshIndexRes
    })

    Option { RefreshIndexResults(results = operationsResults) }
  }

}
