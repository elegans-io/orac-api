package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import java.io._

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings._
import org.elasticsearch.common.xcontent.XContentType

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.{Failure, Success, Try}
import scalaz.Scalaz._

case class SystemIndexManagementServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object SystemIndexManagementService {
  val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  def analyzerFiles: JsonMappingAnalyzersIndexFiles =
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/system/analyzer.json",
      updatePath = "/index_management/json_index_spec/system/update/analyzer.json",
      indexSuffix = "")

  val schemaFiles: List[JsonMappingAnalyzersIndexFiles] = List[JsonMappingAnalyzersIndexFiles](
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/system/user.json",
      updatePath = "/index_management/json_index_spec/system/update/user.json",
      indexSuffix = elasticClient.userIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/system/forward.json",
      updatePath = "/index_management/json_index_spec/system/update/forward.json",
      indexSuffix = elasticClient.forwardIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/system/reconcile.json",
      updatePath = "/index_management/json_index_spec/system/update/reconcile.json",
      indexSuffix = elasticClient.reconcileIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/system/reconcile_history.json",
      updatePath = "/index_management/json_index_spec/system/update/reconcile_history.json",
      indexSuffix = elasticClient.reconcileHistoryIndexSuffix)
  )

  def createIndex(indexSuffix: Option[String] = None) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val analyzerJsonPath: String = analyzerFiles.path
    val analyzerJsonIs: Option[InputStream] = Option{getClass.getResourceAsStream(analyzerJsonPath)}
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "Check the file: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
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
          .setSettings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON))
          .setSource(schemaJson, XContentType.JSON).get()

      item.indexSuffix + "(" + fullIndexName + ", " + createIndexRes.isAcknowledged.toString + ")"
    })

    val message = "SystemIndexCreation: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def removeIndex(indexSuffix: Option[String] = None) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    if (! elasticClient.enableDeleteIndex) {
      val message: String = "operation is not allowed, contact system administrator"
      throw SystemIndexManagementServiceException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix

      val deleteIndexRes: DeleteIndexResponse =
        client.admin().indices().prepareDelete(fullIndexName).get()

      item.indexSuffix + "(" + fullIndexName + ", " + deleteIndexRes.isAcknowledged.toString + ")"

    })

    val message = "SystemIndexDeletion: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def openCloseIndex(indexSuffix: Option[String] = None,
                     operation: String): Future[List[OpenCloseIndex]] = Future {
    val client: TransportClient = elasticClient.getClient
    schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
      operation match {
        case "close" =>
          val closeIndexResponse: CloseIndexResponse = client.admin().indices().prepareClose(fullIndexName).get()
          OpenCloseIndex(indexName = elasticClient.indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = closeIndexResponse.isAcknowledged)
        case "open" =>
          val openIndexResponse: OpenIndexResponse = client.admin().indices().prepareOpen(fullIndexName).get()
          OpenCloseIndex(indexName = elasticClient.indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = openIndexResponse.isAcknowledged)
        case _ => throw IndexManagementServiceException("operation not supported on index: " + operation)
      }
    })
  }

  def checkIndexStatus(indexSuffix: Option[String] = None) : List[(String, String, String, Boolean)] = {
    val client: TransportClient = elasticClient.getClient

    schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
      Try(client.admin.indices.prepareGetMappings(fullIndexName).get()) match {
        case Success(getMappingsReq) =>
          val check = getMappingsReq.mappings.containsKey(fullIndexName)
          (fullIndexName, elasticClient.indexName, item.indexSuffix, check)
        case Failure(e) =>
          log.error("SystemIndex check failed: " + fullIndexName + " : " + e)
          (fullIndexName, elasticClient.indexName, item.indexSuffix, false)
      }
    })
  }

  def checkIndex(indexSuffix: Option[String] = None) : Future[Option[IndexManagementResponse]] = Future {
    val message = "SystemIndexCheck: " + checkIndexStatus(indexSuffix).map {
      case (fullIndexName, _, suffix, result) =>
        suffix + " (" + fullIndexName + ", " + result +  ")"
    }.mkString(" ")
    Option { IndexManagementResponse(message) }
  }

  def updateIndexSettings(indexSuffix: Option[String] = None) :
  Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    val analyzerJsonPath: String = analyzerFiles.path
    val analyzerJsonIs: Option[InputStream] = Option{getClass.getResourceAsStream(analyzerJsonPath)}
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "Check the file: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
      val updateIndexRes =
        client.admin().indices().prepareUpdateSettings().setIndices(fullIndexName)
          .setSettings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON)).get()
      item.indexSuffix + "(" + fullIndexName + ", " + updateIndexRes.isAcknowledged.toString + ")"
    })

    val message = "SystemIndexSettingsUpdate: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def updateIndexMappings(indexSuffix: Option[String] = None) :
  Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient
    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val jsonInStream: Option[InputStream] = Option{getClass.getResourceAsStream(item.updatePath)}

      val schemaJson: String = jsonInStream match {
        case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
        case _ =>
          val message = "Check the file: (" + item.updatePath + ")"
          throw new FileNotFoundException(message)
      }

      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix

      val updateIndexRes  =
        client.admin().indices().preparePutMapping().setIndices(fullIndexName)
          .setType(item.indexSuffix)
          .setSource(schemaJson, XContentType.JSON).get()

      item.indexSuffix + "(" + fullIndexName + ", " + updateIndexRes.isAcknowledged.toString + ")"
    })

    val message = "SystemIndexUpdate: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }


  def refreshIndexes(indexSuffix: Option[String] = None) : Future[Option[RefreshIndexResults]] = Future {
    val operationsResults: List[RefreshIndexResult] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t === item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = elasticClient.indexName + "." + item.indexSuffix
      val refreshIndexRes: RefreshIndexResult = elasticClient.refreshIndex(fullIndexName)
      if (refreshIndexRes.failed_shards_n > 0) {
        val indexRefreshMessage = item.indexSuffix + "(" + fullIndexName + ", " + refreshIndexRes.failed_shards_n + ")"
        throw SystemIndexManagementServiceException(indexRefreshMessage)
      }

      refreshIndexRes
    })

    Option { RefreshIndexResults(results = operationsResults) }
  }

  def indices: Future[List[String]] = Future {
    val indicesRes = elasticClient.getClient
      .admin.cluster.prepareState.get.getState.getMetaData.getIndices.asScala
    indicesRes.map(x => x.key).toList
  }


}
