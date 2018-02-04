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
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object SystemIndexManagementService {
  val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val schemaFiles: List[JsonIndexFiles] = List[JsonIndexFiles](
    JsonIndexFiles(path = "/index_management/json_index_spec/system/user.json",
      updatePath = "/index_management/json_index_spec/system/update/user.json",
      indexSuffix = elasticClient.userIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/system/forward.json",
      updatePath = "/index_management/json_index_spec/system/update/forward.json",
      indexSuffix = elasticClient.forwardIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/system/reconcile.json",
      updatePath = "/index_management/json_index_spec/system/update/reconcile.json",
      indexSuffix = elasticClient.reconcileIndexSuffix),
    JsonIndexFiles(path = "/index_management/json_index_spec/system/reconcile_history.json",
      updatePath = "/index_management/json_index_spec/system/update/reconcile_history.json",
      indexSuffix = elasticClient.reconcileHistoryIndexSuffix)
  )

  def createIndex : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val operationsMessage: List[String] = schemaFiles.map(item => {
      val jsonInStream: Option[InputStream] = Option {getClass.getResourceAsStream(item.path)}

      val schemaJson = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.path + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix

      val createIndexRes: CreateIndexResponse =
        client.admin().indices().prepareCreate(fullIndexName)
          .setSource(schemaJson, XContentType.JSON).get()

      item.indexSuffix + "(" + fullIndexName + ", " + createIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexCreation: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def removeIndex : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    if (! elasticClient.enableDeleteIndex) {
      val message: String = "operation is not allowed, contact system administrator"
      throw new Exception(message)
    }

    val operationsMessage: List[String] = schemaFiles.map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix

      val deleteIndexRes: DeleteIndexResponse =
        client.admin().indices().prepareDelete(fullIndexName).get()

      item.indexSuffix + "(" + fullIndexName + ", " + deleteIndexRes.isAcknowledged.toString + ")"

    })

    val message = "IndexDeletion: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def checkIndex : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val operations_message: List[String] = schemaFiles.map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
      val getMappingsReq = client.admin.indices.prepareGetMappings(fullIndexName).get()
      val check = getMappingsReq.mappings.containsKey(fullIndexName)
      item.indexSuffix + "(" + fullIndexName + ", " + check + ")"
    })

    val message = "IndexCheck: " + operations_message.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def checkIndexStatus : Boolean = {
    val client: TransportClient = elasticClient.getClient

    val operationsMessage: List[Boolean] = schemaFiles.map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
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

  def updateIndex : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val operationsMessage: List[String] = schemaFiles.map(item => {
      val jsonInStream: Option[InputStream] = Option{getClass.getResourceAsStream(item.updatePath)}
      val schemaJson: String = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.path + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix

      val updateIndexRes  =
        client.admin().indices().preparePutMapping().setIndices(fullIndexName)
          .setType(item.indexSuffix)
          .setSource(schemaJson, XContentType.JSON).get()

      item.indexSuffix + "(" + fullIndexName + ", " + updateIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexUpdate: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def refreshIndexes : Future[Option[RefreshIndexResults]] = Future {
    val operationsResults: List[RefreshIndexResult] = schemaFiles.map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
      val refreshIndexRes: RefreshIndexResult = elasticClient.refreshIndex(fullIndexName)
      if (refreshIndexRes.failed_shards_n > 0) {
        val indexRefreshMessage = item.indexSuffix + "(" + fullIndexName + ", " + refreshIndexRes.failed_shards_n + ")"
        throw new Exception(indexRefreshMessage)
      }

      refreshIndexRes
    })

    Option { RefreshIndexResults(results = operationsResults) }
  }

  def getIndices: Future[List[String]] = Future {
    val indicesRes = elasticClient.getClient
      .admin.cluster.prepareState.get.getState.getMetaData.getIndices.asScala
    indicesRes.map(x => x.key).toList
  }


}
