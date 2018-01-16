package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import io.elegans.orac.entities._

import scala.concurrent.Future
import scala.collection.immutable.{List, Map}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}

import scala.collection.JavaConverters._
import org.elasticsearch.rest.RestStatus
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.services.RecommendationService._
import org.elasticsearch.search.SearchHit
import org.elasticsearch.action.search.SearchResponse

import scala.concurrent.ExecutionContext.Implicits.global
import io.elegans.orac.tools._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.sort.SortOrder

/**
  * Implements functions, eventually used by ActionResource
  */
object ActionService {
  val elastic_client: ActionElasticClient.type = ActionElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  def getIndexName(index_name: String, suffix: Option[String] = None): String = {
    index_name + "." + suffix.getOrElse(elastic_client.action_index_suffix)
  }

  def create(index_name: String, creator_user_id: String, document: Action,
             refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val timestamp: Long = document.timestamp.getOrElse(Time.getTimestampMillis)
    val id: String = document.id
      .getOrElse(Checksum.sha512(document.item_id + document.user_id + document.name + timestamp))

    builder.field("id", id)
    builder.field("name", document.name)
    builder.field("user_id", document.user_id)
    builder.field("item_id", document.item_id)
    builder.field("timestamp", timestamp)
    builder.field("creator_uid", creator_user_id)

    if(document.score.isDefined) {
      builder.field("score", document.score.get)
    }

    if(document.ref_url.isDefined) {
      builder.field("ref_url", document.ref_url.get)
    }

    if (document.ref_recommendation.isDefined) {
      builder.field("ref_recommendation", document.ref_recommendation.get)
    }
    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(getIndexName(index_name))
      .setType(elastic_client.action_index_suffix)
      .setCreate(true)
      .setId(id)
      .setSource(builder).get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName(index_name))
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + index_name + ")")
      }
    }

    val doc_result: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    if(forwardService.forwardEnabled(index_name)) {
      val forward = Forward(doc_id = id, index = Some(index_name),
        `type` = ForwardType.action,
        operation = ForwardOperationType.create, retry = Option{10})
      forwardService.create(index_name = index_name, document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def update(index_name: String, id: String, document: UpdateAction,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.user_id match {
      case Some(t) => builder.field("user_id", t)
      case None => ;
    }

    document.item_id match {
      case Some(t) => builder.field("item_id", t)
      case None => ;
    }

    document.timestamp match {
      case Some(t) => builder.field("timestamp", t)
      case None => ;
    }

    document.score match {
      case Some(t) => builder.field("score", t)
      case None => ;
    }

    document.creator_uid match {
      case Some(t) => builder.field("creator_uid", t)
      case None => ;
    }

    document.ref_url match {
      case Some(t) => builder.field("ref_url", t)
      case None => ;
    }

    document.ref_recommendation match {
      case Some(t) => builder.field("ref_recommendation", t)
      case None => ;
    }

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(index_name))
      .setType(elastic_client.action_index_suffix).setId(id)
      .setDoc(builder)
      .get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName(index_name))
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + index_name + ")")
      }
    }

    val doc_result: UpdateDocumentResult = UpdateDocumentResult(index = response.getIndex,
      dtype = response.getType,
      id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    if(forwardService.forwardEnabled(index_name)) {
      val forward = Forward(doc_id = id, index = Some(index_name),
        `type` = ForwardType.action,
        operation = ForwardOperationType.update, retry = Option{10})
      forwardService.create(index_name = index_name, document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def delete(index_name: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(index_name))
      .setType(elastic_client.action_index_suffix).setId(id).get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName(index_name))
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + index_name + ")")
      }
    }

    val doc_result: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    if(forwardService.forwardEnabled(index_name)) {
      val forward = Forward(doc_id = id, index = Some(index_name),
        `type` = ForwardType.action,
        operation = ForwardOperationType.delete, retry = Option{10})
      forwardService.create(index_name = index_name, document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def read(index_name: String, ids: List[String]): Future[Option[Actions]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName(index_name), elastic_client.action_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + index_name + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents : List[Action] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val user_id : String = source.get("user_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val item_id : String = source.get("item_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val timestamp : Option[Long] = source.get("timestamp") match {
        case Some(t) => Option{ t.asInstanceOf[Long] }
        case None => Option{0}
      }

      val score : Option[Double] = source.get("score") match {
        case Some(t) => Option{ t.asInstanceOf[Double] }
        case None => Option{0.0}
      }

      val creator_uid : Option[String] = source.get("creator_uid") match {
        case Some(t) => Option{t.asInstanceOf[String]}
        case None => Option{""}
      }

      val ref_url : Option[String] = source.get("ref_url") match {
        case Some(t) => Option{t.asInstanceOf[String]}
        case None => Option.empty[String]
      }

      val ref_recommendation : Option[String] = source.get("ref_recommendation") match {
        case Some(t) => Option{t.asInstanceOf[String]}
        case None => Option.empty[String]
      }

      val document : Action = Action(id = Option { id }, name = name, user_id = user_id, item_id = item_id,
        timestamp = timestamp, score = score, creator_uid = creator_uid, ref_url = ref_url,
        ref_recommendation = ref_recommendation)
      document
    })

    Option{ Actions(items = documents) }
  }

  def getAllDocuments(index_name: String, search: Option[UpdateAction] = Option.empty, keepAlive: Long = 60000):
    Iterator[Action] = {
    val qb: QueryBuilder = if(search.isEmpty) {
      QueryBuilders.matchAllQuery()
    } else {
      val document = search.get
      val bool_query_builder = QueryBuilders.boolQuery()
      if (document.item_id.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("item_id", document.item_id.get))
      if (document.user_id.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("user_id", document.user_id.get))
      if (document.name.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("name", document.name.get))
      if (document.score.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("score", document.score.get))
      if (document.ref_recommendation.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("ref_recommendation", document.ref_recommendation.get))
      bool_query_builder
    }

    var scrollResp: SearchResponse = elastic_client.get_client()
      .prepareSearch(getIndexName(index_name))
      .addSort("timestamp", SortOrder.DESC)
      .setScroll(new TimeValue(keepAlive))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually{
      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val name: String = source.get("name") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val user_id : String = source.get("user_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val item_id : String = source.get("item_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val timestamp : Option[Long] = source.get("timestamp") match {
          case Some(t) => Option{ t.asInstanceOf[Long] }
          case None => Option{0}
        }

        val score : Option[Double] = source.get("score") match {
          case Some(t) => Option{ t.asInstanceOf[Double] }
          case None => Option{0.0}
        }

        val creator_uid : Option[String] = source.get("creator_uid") match {
          case Some(t) => Option{t.asInstanceOf[String]}
          case None => Option{""}
        }

        val ref_url : Option[String] = source.get("ref_url") match {
          case Some(t) => Option{t.asInstanceOf[String]}
          case None => Option.empty[String]
        }

        val ref_recommendation : Option[String] = source.get("ref_recommendation") match {
          case Some(t) => Option{t.asInstanceOf[String]}
          case None => Option.empty[String]
        }

        val document : Action = Action(id = Option { id }, name = name, user_id = user_id, item_id = item_id,
          timestamp = timestamp, score = score, creator_uid = creator_uid, ref_url = ref_url,
          ref_recommendation = ref_recommendation)

        document
      })

      scrollResp = elastic_client.get_client().prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }

  def read_all(index_name: String, search: Option[UpdateAction] = Option.empty): Future[Option[Actions]] = Future {
    Option{
      Actions(items = getAllDocuments(index_name, search).toList)
    }
  }

}