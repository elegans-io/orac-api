package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 30/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
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
import scala.util.{Failure, Success}
import scalaz.Scalaz._

/**
  * Implements forwarding functions
  */

object  ForwardService {
  val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  val forwardingDestinations: Map[String, List[(ForwardingDestination, AbstractForwardingImplService)]] =
    elasticClient.forwarding.map(forwardingIndex => {
      val forwarders = forwardingIndex._2.map(item => {
        val forwarding_destination =
          ForwardingDestination(index = forwardingIndex._1, url = item._1,
            service_type = SupportedForwardingServicesImpl.getValue(item._2),
            item_info_id = item._3)
        val forwarder = ForwardingServiceImplFactory.apply(forwarding_destination)
        (forwarding_destination, forwarder)
      })
      (forwardingIndex._1, forwarders)
    })

  def forwardEnabled(indexName: String): Boolean = {
    forwardingDestinations.contains(indexName)
  }

  def fullIndexName: String = {
    elasticClient.indexName + "." + elasticClient.forwardIndexSuffix
  }

  def create(indexName: String, document: Forward, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val id: String = document.id
      .getOrElse(Checksum.sha512(document.doc_id + document.index + document.item_type +
        document.operation + RandomNumbers.long))
    builder.field("id", id)
    builder.field("doc_id", document.doc_id)
    builder.field("index", indexName)
    builder.field("item_type", document.item_type)
    builder.field("operation", document.operation)
    builder.field("retry", document.retry.getOrElse(10L))
    val timestamp: Long = Time.timestampMillis
    builder.field("timestamp", timestamp)

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(fullIndexName)
      .setType(elasticClient.forwardIndexSuffix)
      .setId(id)
      .setCreate(true)
      .setSource(builder).get()

    if (refresh =/= 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName)
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + fullIndexName + ")")
      }
    }

    val docResult: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    Option {docResult}
  }

  def deleteAll(indexName: String): Future[Option[DeleteDocumentsResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val qb = QueryBuilders.termQuery("index", indexName)
    //val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    val response: BulkByScrollResponse =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client).setMaxRetries(10)
        .source(fullIndexName)
        .filter(qb)
        .filter(QueryBuilders.typeQuery(elasticClient.forwardIndexSuffix))
        .get()

    val deleted: Long = response.getDeleted

    val result: DeleteDocumentsResult = DeleteDocumentsResult(message = "delete", deleted = deleted)
    Option {result}
  }

  def forwardDeleteAll(indexName: String): Future[Unit] = Future {
    val itemIterator = itemService.allDocuments(indexName)
    itemIterator.map(doc => {
      val forward = Forward(doc_id = doc.id, index = Some(indexName),
        item_type = ForwardType.item,
        operation = ForwardOperationType.delete)
      forward
    }).foreach(forward => {
      val result = create(indexName, forward, 0)
      result.onComplete {
        case Success(t) =>
          if(! t.get.created) {
            log.error("forward entry was not created")
          }
        case Failure(e) =>
          log.error("can't create forward entry: " + forward + " : " + e.printStackTrace)
      }
    })

    val oracUserIterator = oracUserService.allDocuments(indexName)
    oracUserIterator.map(doc => {
      val forward = Forward(doc_id = doc.id, index = Some(indexName),
        item_type = ForwardType.orac_user,
        operation = ForwardOperationType.delete)
      forward
    }).foreach(forward => {
      val result = create(indexName, forward, 0)
      result.onComplete {
        case Success(t) =>
          if(! t.get.created) {
            log.error("forward entry was not created")
          }
        case Failure(e) =>
          log.error("can't create forward entry: " + forward + " : " + e.printStackTrace)
      }
    })

    val actionIterator = actionService.allDocuments(indexName)
    actionIterator.map(doc => {
      val forward = Forward(doc_id = doc.id.get, index = Some(indexName),
        item_type = ForwardType.action,
        operation = ForwardOperationType.delete)
      forward
    }).foreach(forward => {
      val result = create(indexName, forward, 0)
      result.onComplete {
        case Success(t) =>
          if(! t.get.created) {
            log.error("forward entry was not created")
          }
        case Failure(e) =>
          log.error("can't create forward entry: " + forward + " : " + e.printStackTrace)
      }
    })
  }

  def forwardCreateAll(indexName: String): Future[Unit] = Future {
    val itemIterator = itemService.allDocuments(indexName)
    itemIterator.map(doc => {
      val forward = Forward(doc_id = doc.id, index = Some(indexName),
        item_type = ForwardType.item,
        operation = ForwardOperationType.create)
      forward
    }).foreach(forward => {
      val result = create(indexName, forward, 0)
      result.onComplete {
        case Success(t) =>
          if(! t.get.created) {
            log.error("forward entry was not created")
          } else {
            log.debug("forward entry created: " + forward)
          }
        case Failure(e) =>
          log.error("can't create forward entry: " + forward + " : " + e.printStackTrace)
      }
    })

    val oracUserIterator = oracUserService.allDocuments(indexName)
    oracUserIterator.map(doc => {
      val forward = Forward(doc_id = doc.id, index = Some(indexName),
        item_type = ForwardType.orac_user,
        operation = ForwardOperationType.create)
      forward
    }).foreach(forward => {
      val result = create(indexName, forward, 0)
      result.onComplete {
        case Success(t) =>
          if(! t.get.created) {
            log.error("forward entry was not created")
          } else {
            log.debug("forward entry created: " + forward)
          }
        case Failure(e) =>
          log.error("can't create forward entry: " + forward + " : " + e.printStackTrace)
      }
    })

    val actionIterator = actionService.allDocuments(indexName)
    actionIterator.map(doc => {
      val forward = Forward(doc_id = doc.id.get, index = Some(indexName),
        item_type = ForwardType.action,
        operation = ForwardOperationType.create)
      forward
    }).foreach(forward => {
      val result = create(indexName, forward, 0)
      result.onComplete {
        case Success(t) =>
          if(! t.get.created) {
            log.error("forward entry was not created")
          } else {
            log.debug("forward entry created: " + forward)
          }
        case Failure(e) =>
          log.error("can't create forward entry: " + forward + " : " + e.printStackTrace)
      }
    })
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(fullIndexName)
      .setType(elasticClient.forwardIndexSuffix).setId(id).get()

    if (refresh =/= 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName)
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status =/= RestStatus.NOT_FOUND
    )

    log.debug("Delete forward item: " + id)
    Option {docResult}
  }

  def read(indexName: String, ids: List[String]): Future[Option[List[Forward]]] = {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(fullIndexName, elasticClient.forwardIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + fullIndexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : List[Forward] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists)
      .filter(_.getIndex === indexName).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val doc_id: String = source.get("doc_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val index: Option[String] = source.get("index") match {
        case Some(t) => Some(t.asInstanceOf[String])
        case None => Some("")
      }

      val item_type: ForwardType.Forward = source.get("item_type") match {
        case Some(t) => ForwardType.getValue(t.asInstanceOf[String])
        case None => ForwardType.unknown
      }

      val operation: ForwardOperationType.ForwardOperation = source.get("operation") match {
        case Some(t) => ForwardOperationType.getValue(t.asInstanceOf[String])
        case None => ForwardOperationType.unknown
      }

      val retry : Option[Long] = source.get("retry") match {
        case Some(t) => Option{t.asInstanceOf[Integer].toLong}
        case None => Option{0}
      }

      val timestamp : Option[Long] = source.get("timestamp") match {
        case Some(t) => Option{t.asInstanceOf[Long]}
        case None => Option{0}
      }

      val document = Forward(id = Option{id}, doc_id = doc_id, index = index, item_type = item_type,
        operation = operation, retry = retry, timestamp = timestamp)
      document
    })

    Future { Option { documents } }
  }

  def allDocuments(keepAlive: Long = 60000): Iterator[Forward] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(fullIndexName)
      .addSort("timestamp", SortOrder.ASC)
      .setScroll(new TimeValue(keepAlive))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually {

      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val doc_id: String = source.get("doc_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val index: Option[String] = source.get("index") match {
          case Some(t) => Some(t.asInstanceOf[String])
          case None => Some("")
        }

        val item_type: ForwardType.Forward = source.get("item_type") match {
          case Some(t) => ForwardType.getValue(t.asInstanceOf[String])
          case None => ForwardType.unknown
        }

        val operation: ForwardOperationType.ForwardOperation = source.get("operation") match {
          case Some(t) => ForwardOperationType.getValue(t.asInstanceOf[String])
          case None => ForwardOperationType.unknown
        }

        val retry : Option[Long] = source.get("retry") match {
          case Some(t) => Option{t.asInstanceOf[Integer].toLong}
          case None => Option{0}
        }

        val timestamp : Option[Long] = source.get("timestamp") match {
          case Some(t) => Option{t.asInstanceOf[Long]}
          case None => Option{0}
        }

        val document = Forward(id = Option{id}, doc_id = doc_id, index = index, item_type = item_type,
          operation = operation, retry = retry, timestamp = timestamp)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()

      (documents, documents.nonEmpty)
    }.takeWhile{case(_, docNonEmpty) => docNonEmpty}.flatMap{case(docs, _) => docs}

    iterator
  }
}
