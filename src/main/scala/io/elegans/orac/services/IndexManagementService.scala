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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

case class IndexManagementServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object IndexManagementService {
  val elasticClient: IndexManagementClient.type = IndexManagementClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  def analyzerFiles(language: String): JsonMappingAnalyzersIndexFiles =
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/" + language + "/analyzer.json",
      updatePath = "/index_management/json_index_spec/" + language + "/update/analyzer.json",
      indexSuffix = "")

  val schemaFiles: List[JsonMappingAnalyzersIndexFiles] = List[JsonMappingAnalyzersIndexFiles](
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/action.json",
      updatePath = "/index_management/json_index_spec/general/update/action.json",
      indexSuffix = elasticClient.actionIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/item.json",
      updatePath = "/index_management/json_index_spec/general/update/item.json",
      indexSuffix = elasticClient.itemIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/item_info.json",
      updatePath = "/index_management/json_index_spec/general/update/item_info.json",
      indexSuffix = elasticClient.itemInfoIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/orac_user.json",
      updatePath = "/index_management/json_index_spec/general/update/orac_user.json",
      indexSuffix = elasticClient.userIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/recommendation.json",
      updatePath = "/index_management/json_index_spec/general/update/recommendation.json",
      indexSuffix = elasticClient.recommendationIndexSuffix),
    JsonMappingAnalyzersIndexFiles(path = "/index_management/json_index_spec/general/recommendation_history.json",
      updatePath = "/index_management/json_index_spec/general/update/recommendation_history.json",
      indexSuffix = elasticClient.recommendationHistoryIndexSuffix)
  )

  def createIndex(indexName: String,
                  indexSuffix: Option[String] = None) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    // extract language from index name
    val indexLanguageRegex = "^(?:(index)_([a-z]{1,256})_([A-Za-z0-9_]{1,256}))$".r
    val indexPatterns = indexName match {
      case indexLanguageRegex(index_pattern, language_pattern, arbitrary_pattern) =>
        (index_pattern, language_pattern, arbitrary_pattern)
      case _ => throw IndexManagementServiceException("index name is not well formed")
    }
    val language: String = indexPatterns._2

    val analyzerJsonPath: String = analyzerFiles(language).path
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

  def removeIndex(indexName: String,
                  indexSuffix: Option[String] = None) : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    if (! elasticClient.enableDeleteIndex) {
      val message: String = "operation is not allowed, contact system administrator"
      throw IndexManagementServiceException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix

      val deleteIndexRes: DeleteIndexResponse =
        client.admin().indices().prepareDelete(fullIndexName).get()

      item.indexSuffix + "(" + fullIndexName + ", " + deleteIndexRes.isAcknowledged.toString + ")"

    })

    val message = "IndexDeletion: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def checkIndexStatus(indexName: String,
                       indexSuffix: Option[String] = None) : List[(String, String, String, Boolean)] = {
    val client: TransportClient = elasticClient.getClient

    schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val getMappingsReq = client.admin.indices.prepareGetMappings(fullIndexName).get()
      val check = getMappingsReq.mappings.containsKey(fullIndexName)
      (fullIndexName, indexName, item.indexSuffix, check)
    })
  }

  def checkIndex(indexName: String,
                 indexSuffix: Option[String] = None) : Future[Option[IndexManagementResponse]] = Future {
    val message = "IndexCheck: " + checkIndexStatus(indexName, indexSuffix).map {
      case (fullIndexName, _, suffix, result) =>
        suffix + " (" + fullIndexName + ", " + result +  ")"
    }.mkString(" ")
    Option { IndexManagementResponse(message) }
  }

  def openCloseIndex(indexName: String, indexSuffix: Option[String] = None,
                     operation: String): Future[List[OpenCloseIndex]] = Future {
    val client: TransportClient = elasticClient.getClient
    schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      operation match {
        case "close" =>
          val closeIndexResponse: CloseIndexResponse = client.admin().indices().prepareClose(fullIndexName).get()
          OpenCloseIndex(indexName = indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = closeIndexResponse.isAcknowledged)
        case "open" =>
          val openIndexResponse: OpenIndexResponse = client.admin().indices().prepareOpen(fullIndexName).get()
          OpenCloseIndex(indexName = indexName, indexSuffix = item.indexSuffix, fullIndexName = fullIndexName,
            operation = operation, acknowledgement = openIndexResponse.isAcknowledged)
        case _ => throw IndexManagementServiceException("operation not supported on index: " + operation)
      }
    })
  }

  def updateIndexSettings(indexName: String, indexSuffix: Option[String] = None) :
  Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    // extract language from index name
    val indexLanguageRegex = "^(?:(index)_([a-z]{1,256})_([A-Za-z0-9_]{1,256}))$".r
    val indexPatterns = indexName match {
      case indexLanguageRegex(index_pattern, language_pattern, arbitrary_pattern) =>
        (index_pattern, language_pattern, arbitrary_pattern)
      case _ => throw IndexManagementServiceException("index name is not well formed")
    }
    val language: String = indexPatterns._2

    val analyzerJsonPath: String = analyzerFiles(language).updatePath
    val analyzerJsonIs: Option[InputStream] = Option{getClass.getResourceAsStream(analyzerJsonPath)}
    val analyzerJson: String = analyzerJsonIs match {
      case Some(stream) => Source.fromInputStream(stream, "utf-8").mkString
      case _ =>
        val message = "file not found: (" + analyzerJsonPath + ")"
        throw new FileNotFoundException(message)
    }

    val operationsMessage: List[String] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val updateIndexRes =
        client.admin().indices().prepareUpdateSettings().setIndices(fullIndexName)
          .setSettings(Settings.builder().loadFromSource(analyzerJson, XContentType.JSON)).get()
      item.indexSuffix + "(" + fullIndexName + ", " + updateIndexRes.isAcknowledged.toString + ")"
    })

    val message = "IndexSettingsUpdate: " + operationsMessage.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def updateIndexMappings(indexName: String, indexSuffix: Option[String] = None) :
  Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elasticClient.getClient

    // extract language from index name
    val indexLanguageRegex = "^(?:(index)_([a-z]{1,256})_([A-Za-z0-9_]{1,256}))$".r
    val indexPatterns = indexName match {
      case indexLanguageRegex(index_pattern, language_pattern, arbitrary_pattern) =>
        (index_pattern, language_pattern, arbitrary_pattern)
      case _ => throw IndexManagementServiceException("index name is not well formed")
    }
    val language: String = indexPatterns._2

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

  def refreshIndexes(indexName: String,
                     indexSuffix: Option[String] = None) : Future[Option[RefreshIndexResults]] = Future {
    val operationsResults: List[RefreshIndexResult] = schemaFiles.filter(item => {
      indexSuffix match {
        case Some(t) => t == item.indexSuffix
        case _ => true
      }
    }).map(item => {
      val fullIndexName = indexName + "." + item.indexSuffix
      val refreshIndexRes: RefreshIndexResult = elasticClient.refreshIndex(fullIndexName)
      if (refreshIndexRes.failed_shards_n > 0) {
        val indexRefreshMessage = item.indexSuffix + "(" + fullIndexName + ", " +
          refreshIndexRes.failed_shards_n + ")"
        throw new Exception(indexRefreshMessage)
      }

      refreshIndexRes
    })

    Option { RefreshIndexResults(results = operationsResults) }
  }

}


