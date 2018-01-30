package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.tools.{Checksum, Time}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.rest.RestStatus

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Implements functions, eventually used by RecommendationHistoryResource
  */
object RecommendationHistoryService {
  val elasticClient: RecommendationElasticClient.type = RecommendationElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  def getIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.recommendationHistoryIndexSuffix)
  }

  def create(indexName: String, creator_user_id: String,
             document: RecommendationHistory, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()


    val accessTimestamp: Long = document.access_timestamp.getOrElse(Time.getTimestampMillis)

    val id: String = document.id
      .getOrElse(Checksum.sha512(document.item_id + document.user_id + document.name +
        document.recommendation_id + document.access_user_id + document.generation_batch +
        accessTimestamp + document.score +
        document.generation_timestamp))

    val accessUserId = document.access_user_id.getOrElse(creator_user_id)

    builder.field("id", id)
    builder.field("recommendation_id", document.recommendation_id)
    builder.field("access_user_id", accessUserId)
    builder.field("name", document.name)
    builder.field("generation_batch", document.generation_batch)
    builder.field("user_id", document.user_id)
    builder.field("item_id", document.item_id)
    builder.field("generation_timestamp", document.generation_timestamp)
    builder.field("access_timestamp", accessTimestamp)
    builder.field("score", document.score)
    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(getIndexName(indexName))
      .setType(elasticClient.recommendationHistoryIndexSuffix)
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

    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateRecommendationHistory, refresh: Int):
      Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.recommendation_id match {
      case Some(t) => builder.field("recommendation_id", t)
      case None => ;
    }

    document.access_user_id match {
      case Some(t) => builder.field("access_user_id", t)
      case None => ;
    }

    document.generation_batch match {
      case Some(t) => builder.field("generation_batch", t)
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

    document.generation_timestamp match {
      case Some(t) => builder.field("generation_timestamp", t)
      case None => ;
    }

    document.access_timestamp match {
      case Some(t) => builder.field("access_timestamp", t)
      case None => ;
    }

    document.score match {
      case Some(t) => builder.field("score", t)
      case None => ;
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(indexName))
      .setType(elasticClient.recommendationHistoryIndexSuffix).setId(id)
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

    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(indexName))
      .setType(elasticClient.recommendationHistoryIndexSuffix).setId(id).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val doc_result: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    Option {doc_result}
  }

  def read(indexName: String, ids: List[String]): Future[Option[RecommendationsHistory]] = Future {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(getIndexName(indexName), elasticClient.recommendationHistoryIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + indexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : List[RecommendationHistory] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val recommendationId: String = source.get("recommendation_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val userId : String = source.get("user_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val accessUserId : Option[String] = source.get("access_user_id") match {
        case Some(t) => Option { t.asInstanceOf[String] }
        case None => Option {""}
      }

      val generationBatch : String = source.get("generation_batch") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val itemId : String = source.get("item_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val generationTimestamp : Long = source.get("generation_timestamp") match {
        case Some(t) => t.asInstanceOf[Long].longValue()
        case None => 0
      }

      val accessTimestamp : Option[Long] = source.get("access_timestamp") match {
        case Some(t) => Option{ t.asInstanceOf[Long].longValue() }
        case None => Option { 0 }
      }

      val score : Double = source.get("score") match {
        case Some(t) => t.asInstanceOf[Double]
        case None => 0.0
      }

      val document = RecommendationHistory(id = Option { id }, recommendation_id = recommendationId,
        name = name, access_user_id = accessUserId,
        user_id = userId, item_id = itemId,
        generation_batch = generationBatch,
        generation_timestamp = generationTimestamp,
        access_timestamp = accessTimestamp, score = score)
      document
    })

    Option{ RecommendationsHistory(items = documents) }
  }

}