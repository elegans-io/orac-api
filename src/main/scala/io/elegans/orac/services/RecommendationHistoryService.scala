package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import java.util

import akka.actor.ActorSystem
import io.elegans.orac.entities._

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.collection.immutable.{List, Map}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.action.delete.{DeleteRequestBuilder, DeleteResponse}
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryAction}
import org.elasticsearch.index.query.{BoolQueryBuilder, InnerHitBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.common.unit._

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import org.elasticsearch.search.SearchHit
import org.elasticsearch.rest.RestStatus

import scala.util.{Failure, Success, Try}
import akka.event.{Logging, LoggingAdapter}
import akka.event.Logging._
import io.elegans.orac.OracActorSystem
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.apache.lucene.search.join._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import io.elegans.orac.tools.{Checksum, Time}

/**
  * Implements functions, eventually used by RecommendationHistoryResource
  */
object RecommendationHistoryService {
  val elastic_client = RecommendationElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  def getIndexName(index_name: String, suffix: Option[String] = None): String = {
    index_name + "." + suffix.getOrElse(elastic_client.recommendation_history_index_suffix)
  }

  def create(index_name: String, creator_user_id: String,
             document: RecommendationHistory, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()


    val access_timestamp: Long = document.access_timestamp.getOrElse(Time.getTimestampEpoc)

    val id: String = document.id
      .getOrElse(Checksum.sha512(document.item_id + document.user_id + document.name +
        document.recommendation_id + document.access_uid + document.generation_batch +
        access_timestamp + document.score +
        document.generation_timestamp))

    val access_uid = document.access_uid.getOrElse(creator_user_id)

    builder.field("id", id)
    builder.field("recommendation_id", document.recommendation_id)
    builder.field("access_uid", access_uid)
    builder.field("name", document.name)
    builder.field("generation_batch", document.generation_batch)
    builder.field("user_id", document.user_id)
    builder.field("item_id", document.item_id)
    builder.field("generation_timestamp", document.generation_timestamp)
    builder.field("access_timestamp", access_timestamp)
    builder.field("score", document.score)
    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(getIndexName(index_name))
      .setType(elastic_client.recommendation_index_suffix)
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

    Option {doc_result}
  }

  def update(index_name: String, id: String, document: UpdateRecommendationHistory, refresh: Int):
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

    document.access_uid match {
      case Some(t) => builder.field("access_uid", t)
      case None => ;
    }

    document.access_uid match {
      case Some(t) => builder.field("access_uid", t)
      case None => ;
    }

    document.generation_batch match {
      case Some(t) => builder.field("generation_batch", t)
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

    val client: TransportClient = elastic_client.get_client()
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(index_name))
      .setType(elastic_client.recommendation_index_suffix).setId(id)
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

    Option {doc_result}
  }

  def delete(index_name: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(index_name))
      .setType(elastic_client.recommendation_index_suffix).setId(id).get()

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

    Option {doc_result}
  }

  def read(index_name: String, ids: List[String]): Future[Option[RecommendationsHistory]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName(index_name), elastic_client.recommendation_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + index_name + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents : List[RecommendationHistory] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val recommendation_id: String = source.get("recommendation_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val user_id : String = source.get("user_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val access_uid : Option[String] = source.get("access_uid") match {
        case Some(t) => Option { t.asInstanceOf[String] }
        case None => Option {""}
      }

      val generation_batch : String = source.get("generation_batch") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val item_id : String = source.get("item_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val generation_timestamp : Long = source.get("generation_timestamp") match {
        case Some(t) => t.asInstanceOf[Integer].longValue()
        case None => 0
      }

      val access_timestamp : Option[Long] = source.get("access_timestamp") match {
        case Some(t) => Option{ t.asInstanceOf[Integer].longValue() }
        case None => Option { 0 }
      }

      val score : Double = source.get("score") match {
        case Some(t) => t.asInstanceOf[Double]
        case None => 0.0
      }

      val document = RecommendationHistory(id = Option { id }, recommendation_id = recommendation_id,
        name = name, access_uid = access_uid,
        user_id = user_id, item_id = item_id,
        generation_batch = generation_batch,
        generation_timestamp = generation_timestamp,
        access_timestamp = access_timestamp, score = score)
      document
    })

    Option{ RecommendationsHistory(items = documents) }
  }

}