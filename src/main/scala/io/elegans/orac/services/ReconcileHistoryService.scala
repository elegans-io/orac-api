package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities.{ReconcileHistory, _}
import io.elegans.orac.tools.{Checksum, Time}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryAction}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._

/**
  * Implements reconciliation functions
  */

object ReconcileHistoryService {
  val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService

  def fullIndexName: String = {
    elasticClient.indexName + "." + elasticClient.reconcileHistoryIndexSuffix
  }

  def create(document:ReconcileHistory, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val timestamp: Long = Time.timestampMillis
    val endTimestamp: Long = document.end_timestamp.getOrElse(timestamp)
    val id: String = document.id
      .getOrElse(Checksum.sha512(document.toString + timestamp + RandomNumbers.long))

    builder.field("id", id)
    builder.field("old_id", document.old_id)
    builder.field("new_id", document.new_id)
    builder.field("index", document.index)
    builder.field("item_type", document.item_type)
    builder.field("retry", document.retry)
    builder.field("end_timestamp", endTimestamp)
    builder.field("insert_timestamp", document.insert_timestamp)

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(fullIndexName)
      .setType(elasticClient.reconcileHistoryIndexSuffix)
      .setId(id)
      .setCreate(true)
      .setSource(builder).get()

    if (refresh =/= 0) {
      val refresh_index = elasticClient.refreshIndex(fullIndexName)
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + fullIndexName + ")")
      }
    }

    val doc_result: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    Option {doc_result}
  }

  def deleteAll(index_name: String): Future[Option[DeleteDocumentsResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val qb = QueryBuilders.termQuery("index", index_name)
    val response: BulkByScrollResponse =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client).setMaxRetries(10)
        .source(fullIndexName)
        .filter(qb)
        .filter(QueryBuilders.typeQuery(elasticClient.reconcileHistoryIndexSuffix))
        .get()

    val deleted: Long = response.getDeleted

    val result: DeleteDocumentsResult = DeleteDocumentsResult(message = "delete", deleted = deleted)
    Option {result}
  }

  def delete(id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(fullIndexName)
      .setType(elasticClient.reconcileHistoryIndexSuffix).setId(id).get()


    if (refresh =/= 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName)
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + fullIndexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status =/= RestStatus.NOT_FOUND
    )

    Option {docResult}
  }

  def read(ids: List[String]): Future[Option[List[ReconcileHistory]]] = {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(fullIndexName, elasticClient.reconcileHistoryIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + fullIndexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : List[ReconcileHistory] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val newId: String = source.get("new_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val oldId: String = source.get("old_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val index: Option[String] = source.get("index") match {
        case Some(t) => Some(t.asInstanceOf[String])
        case None => Some("")
      }

      val item_type: ReconcileType.Reconcile = source.get("item_type") match {
        case Some(t) => ReconcileType.getValue(t.asInstanceOf[String])
        case None => ReconcileType.unknown
      }

      val retry : Long = source.get("retry") match {
        case Some(t) => t.asInstanceOf[Integer].toLong
        case None => 0
      }

      val insertTimestamp : Long = source.get("insert_timestamp") match {
        case Some(t) => t.asInstanceOf[Long]
        case None => 0
      }

      val endTimestamp : Option[Long] = source.get("end_timestamp") match {
        case Some(t) => Option{t.asInstanceOf[Long]}
        case None => Option{0}
      }

      val document = ReconcileHistory(id = Option{id}, new_id = newId, old_id = oldId,
        index = index, item_type = item_type, retry = retry,
        insert_timestamp = insertTimestamp, end_timestamp = endTimestamp)
      document
    })

    Future { Option { documents } }
  }

  def allDocuments: Iterator[ReconcileHistory] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(fullIndexName)
      .addSort("timestamp", SortOrder.ASC)
      .setScroll(new TimeValue(60000))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually {

      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val newId: String = source.get("new_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val oldId: String = source.get("old_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val index: Option[String] = source.get("index") match {
          case Some(t) => Some(t.asInstanceOf[String])
          case None => Some("")
        }

        val item_type: ReconcileType.Reconcile = source.get("item_type") match {
          case Some(t) => ReconcileType.getValue(t.asInstanceOf[String])
          case None => ReconcileType.unknown
        }

        val retry : Long = source.get("retry") match {
          case Some(t) => t.asInstanceOf[Integer].toLong
          case None => 0
        }

        val insertTimestamp : Long = source.get("insert_timestamp") match {
          case Some(t) => t.asInstanceOf[Long]
          case None => 0
        }

        val endTimestamp : Option[Long] = source.get("end_timestamp") match {
          case Some(t) => Option{t.asInstanceOf[Long]}
          case None => Option{0}
        }

        val document = ReconcileHistory(id = Option{id}, new_id = newId, old_id = oldId,
          index = index, item_type = item_type, retry = retry,
          insert_timestamp = insertTimestamp, end_timestamp = endTimestamp)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()

      (documents, documents.nonEmpty)
    }.takeWhile{case(_, docNonEmpty) => docNonEmpty}.flatMap{case(docs, _) => docs}

    iterator
  }
}
