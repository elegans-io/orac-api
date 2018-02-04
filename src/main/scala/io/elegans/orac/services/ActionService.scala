package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.services.RecommendationService._
import io.elegans.orac.tools._
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Implements functions, eventually used by ActionResource
  */
object ActionService {
  val elasticClient: ActionElasticClient.type = ActionElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  def getIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.actionIndexSuffix)
  }

  def create(indexName: String, creatorUserId: String, document: Action,
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
    builder.field("creator_uid", creatorUserId)

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

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(getIndexName(indexName))
      .setType(elasticClient.actionIndexSuffix)
      .setCreate(true)
      .setId(id)
      .setSource(builder).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        `type` = ForwardType.action,
        operation = ForwardOperationType.create, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateAction,
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

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(indexName))
      .setType(elasticClient.actionIndexSuffix).setId(id)
      .setDoc(builder)
      .get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: UpdateDocumentResult = UpdateDocumentResult(index = response.getIndex,
      dtype = response.getType,
      id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        `type` = ForwardType.action,
        operation = ForwardOperationType.update, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(indexName))
      .setType(elasticClient.actionIndexSuffix).setId(id).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        `type` = ForwardType.action,
        operation = ForwardOperationType.delete, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def read(indexName: String, ids: List[String]): Future[Option[Actions]] = Future {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(getIndexName(indexName), elasticClient.actionIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + indexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : List[Action] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val userId : String = source.get("user_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val itemId : String = source.get("item_id") match {
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

      val creatorUid : Option[String] = source.get("creator_uid") match {
        case Some(t) => Option{t.asInstanceOf[String]}
        case None => Option{""}
      }

      val refUrl : Option[String] = source.get("ref_url") match {
        case Some(t) => Option{t.asInstanceOf[String]}
        case None => Option.empty[String]
      }

      val refRecommendation : Option[String] = source.get("ref_recommendation") match {
        case Some(t) => Option{t.asInstanceOf[String]}
        case None => Option.empty[String]
      }

      val document : Action = Action(id = Option { id }, name = name, user_id = userId, item_id = itemId,
        timestamp = timestamp, score = score, creator_uid = creatorUid, ref_url = refUrl,
        ref_recommendation = refRecommendation)
      document
    })

    Option{ Actions(items = documents) }
  }

  def getAllDocuments(indexName: String, search: Option[UpdateAction] = Option.empty, keepAlive: Long = 60000):
    Iterator[Action] = {
    val qb: QueryBuilder = if(search.isEmpty) {
      QueryBuilders.matchAllQuery()
    } else {
      val document = search.get
      val boolQueryBuilder = QueryBuilders.boolQuery()
      if (document.item_id.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("item_id", document.item_id.get))
      if (document.user_id.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("user_id", document.user_id.get))
      if (document.name.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("name", document.name.get))
      if (document.score.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("score", document.score.get))
      if (document.ref_recommendation.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("ref_recommendation", document.ref_recommendation.get))
      boolQueryBuilder
    }

    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(getIndexName(indexName))
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

        val userId : String = source.get("user_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val itemId : String = source.get("item_id") match {
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

        val creatorUid : Option[String] = source.get("creator_uid") match {
          case Some(t) => Option{t.asInstanceOf[String]}
          case None => Option{""}
        }

        val refUrl : Option[String] = source.get("ref_url") match {
          case Some(t) => Option{t.asInstanceOf[String]}
          case None => Option.empty[String]
        }

        val refRecommendation : Option[String] = source.get("ref_recommendation") match {
          case Some(t) => Option{t.asInstanceOf[String]}
          case None => Option.empty[String]
        }

        val document : Action = Action(id = Option { id }, name = name, user_id = userId, item_id = itemId,
          timestamp = timestamp, score = score, creator_uid = creatorUid, ref_url = refUrl,
          ref_recommendation = refRecommendation)

        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile{case(_, docNonEmpty) => docNonEmpty}.flatMap{case(docs, _) => docs}
    iterator
  }

  def readAll(indexName: String, search: Option[UpdateAction] = Option.empty): Future[Option[Actions]] = Future {
    Option{
      Actions(items = getAllDocuments(indexName, search).toList)
    }
  }

}