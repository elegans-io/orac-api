package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.tools._
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
import scalaz.Scalaz._

/**
  * Implements functions, eventually used by RecommendationResource
  */
abstract class AbstractUserRecommendationService {
  val elasticClient: RecommendationElasticClient.type = RecommendationElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val recommendationHistoryService: RecommendationHistoryService.type = RecommendationHistoryService

  private[this] def fullIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.recommendationIndexSuffix)
  }

  def create(indexName: String, document: Recommendation, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val id: String = document.id
      .getOrElse(Checksum.sha512(document.item_id +
        document.user_id + document.name +
        document.generation_batch +
        document.score +
        document.generation_timestamp + RandomNumbers.long))

    builder.field("id", id)
    builder.field("name", document.name)
    builder.field("algorithm", document.algorithm)
    builder.field("user_id", document.user_id)
    builder.field("item_id", document.item_id)
    builder.field("generation_batch", document.generation_batch)
    builder.field("generation_timestamp", document.generation_timestamp)
    builder.field("score", document.score)
    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(fullIndexName(indexName))
      .setType(elasticClient.recommendationIndexSuffix)
      .setId(id)
      .setCreate(true)
      .setSource(builder).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateRecommendation, refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.algorithm match {
      case Some(t) => builder.field("algorithm", t)
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

    document.generation_batch match {
      case Some(t) => builder.field("generation_batch", t)
      case None => ;
    }

    document.generation_timestamp match {
      case Some(t) => builder.field("generation_timestamp", t)
      case None => ;
    }

    document.score match {
      case Some(t) => builder.field("score", t)
      case None => ;
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(fullIndexName(indexName))
      .setType(elasticClient.recommendationIndexSuffix).setId(id)
      .setDoc(builder)
      .get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: UpdateDocumentResult = UpdateDocumentResult(index = response.getIndex,
      dtype = response.getType,
      id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(fullIndexName(indexName))
      .setType(elasticClient.recommendationIndexSuffix).setId(id).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status =/= RestStatus.NOT_FOUND
    )

    Option {docResult}
  }

  def read(indexName: String, access_user_id: String, ids: List[String]): Future[Option[Recommendations]] = Future {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(fullIndexName(indexName), elasticClient.recommendationIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + indexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : Array[(Recommendation, RecommendationHistory)] = response.getResponses
      .filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val algorithm: String = source.get("algorithm") match {
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

      val generationBatch : String = source.get("generation_batch") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val generationTimestamp : Long = source.get("generation_timestamp") match {
        case Some(t) => t.asInstanceOf[Long]
        case None => 0
      }

      val score : Double = source.get("score") match {
        case Some(t) => t.asInstanceOf[Double]
        case None => 0.0
      }

      val recommendation = Recommendation(id = Option { id }, name = name,
        algorithm = algorithm,
        user_id = userId, item_id = itemId,
        generation_batch = generationBatch,
        generation_timestamp = generationTimestamp, score = score)

      val accessTimestamp: Option[Long] = Option{ Time.timestampMillis }

      val recommendationHistory = RecommendationHistory(id = Option.empty[String], recommendation_id = id,
        name = name, access_user_id = Option { access_user_id },
        algorithm = algorithm,
        user_id = userId, item_id = itemId,
        generation_batch = generationBatch,
        generation_timestamp = generationTimestamp,
        access_timestamp = accessTimestamp, score = score)

      (recommendation, recommendationHistory)
    })

    documents.map(_._2).foreach(recomm => {
      recommendationHistoryService.create(indexName, access_user_id, recomm, 0)
    })

    val recommendations = Option{ Recommendations(items = documents.map(_._1).sortBy(- _.score)) }
    recommendations
  }

}