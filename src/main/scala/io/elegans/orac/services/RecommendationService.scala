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
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, QueryBuilders}

import scala.collection.JavaConverters._
import org.elasticsearch.search.SearchHit
import org.elasticsearch.rest.RestStatus
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem

import scala.concurrent.ExecutionContext.Implicits.global
import io.elegans.orac.tools.{Checksum, Time}
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.search.sort.SortOrder

/**
  * Implements functions, eventually used by RecommendationResource
  */
object RecommendationService {
  val elastic_client: RecommendationElasticClient.type = RecommendationElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val recommendationHistoryService: RecommendationHistoryService.type = RecommendationHistoryService
  val forwardService: ForwardService.type = ForwardService

  def getIndexName(index_name: String, suffix: Option[String] = None): String = {
    index_name + "." + suffix.getOrElse(elastic_client.recommendation_index_suffix)
  }

  def create(index_name: String, document: Recommendation,
             refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val id: String = document.id
      .getOrElse(Checksum.sha512(document.item_id +
        document.user_id + document.name +
        document.generation_batch +
        document.score +
        document.generation_timestamp + RandomNumbers.getLong))

    builder.field("id", id)
    builder.field("name", document.name)
    builder.field("user_id", document.user_id)
    builder.field("item_id", document.item_id)
    builder.field("generation_batch", document.generation_batch)
    builder.field("generation_timestamp", document.generation_timestamp)
    builder.field("score", document.score)
    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(getIndexName(index_name))
      .setType(elastic_client.recommendation_index_suffix)
      .setId(id)
      .setCreate(true)
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

  def update(index_name: String, id: String, document: UpdateRecommendation,
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

  def read(index_name: String, access_user_id: String,
           ids: List[String]): Future[Option[Recommendations]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName(index_name), elastic_client.recommendation_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + index_name + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents : Array[(Recommendation, RecommendationHistory)] = response.getResponses
      .filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

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

      val generation_batch : String = source.get("generation_batch") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val generation_timestamp : Long = source.get("generation_timestamp") match {
        case Some(t) => t.asInstanceOf[Long]
        case None => 0
      }

      val score : Double = source.get("score") match {
        case Some(t) => t.asInstanceOf[Double]
        case None => 0.0
      }

      val recommendation = Recommendation(id = Option { id }, name = name, user_id = user_id, item_id = item_id,
        generation_batch = generation_batch,
        generation_timestamp = generation_timestamp, score = score)

      val access_timestamp: Option[Long] = Option{ Time.getTimestampMillis }

      val recommendation_history = RecommendationHistory(id = Option.empty[String], recommendation_id = id,
        name = name, access_user_id = Option { access_user_id },
        user_id = user_id, item_id = item_id,
        generation_batch = generation_batch,
        generation_timestamp = generation_timestamp,
        access_timestamp = access_timestamp, score = score)

      (recommendation, recommendation_history)
    })

    documents.map(_._2).foreach(recomm => {
      recommendationHistoryService.create(index_name, access_user_id, recomm, 0)
    })

    val recommendations = Option{ Recommendations(items = documents.map(_._1).sortBy(- _.score)) }
    recommendations
  }

  def getAllDocuments(index_name: String,
                      search: Option[UpdateRecommendation] = Option.empty): Iterator[Recommendation] = {
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
      if (document.generation_batch.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("generation_batch", document.generation_batch.get))
      if (document.generation_timestamp.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("generation_timestamp", document.generation_timestamp.get))
      if (document.score.isDefined)
        bool_query_builder.filter(QueryBuilders.termQuery("score", document.score.get))
      bool_query_builder
    }

    var scrollResp: SearchResponse = elastic_client.get_client()
      .prepareSearch(getIndexName(index_name))
      .addSort("generation_timestamp", SortOrder.DESC)
      .setScroll(new TimeValue(60000))
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

        val generation_timestamp : Long = source.get("generation_timestamp") match {
          case Some(t) => t.asInstanceOf[Long]
          case None => 0
        }

        val score : Double = source.get("score") match {
          case Some(t) => t.asInstanceOf[Double]
          case None => 0.0
        }

        val generation_batch : String = source.get("generation_batch") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val document : Recommendation = Recommendation(id = Option { id }, name = name, user_id = user_id,
          item_id = item_id, generation_timestamp = generation_timestamp, score = score,
          generation_batch = generation_batch)

        document
      })

      scrollResp = elastic_client.get_client().prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }

  def getRecommendationsByUser(index_name: String, access_user_id: String, id: String,
                               from: Int = 0, size: Int = 10): Future[Option[Recommendations]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val search_builder : SearchRequestBuilder = client.prepareSearch(getIndexName(index_name))
      .setTypes(elastic_client.recommendation_index_suffix)
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)

    val bool_query_builder : BoolQueryBuilder = QueryBuilders.boolQuery()
    bool_query_builder.filter(QueryBuilders.termQuery("user_id", id))

    search_builder.setQuery(bool_query_builder)

    val search_response : SearchResponse = search_builder
      .setFrom(from).setSize(size)
      .execute()
      .actionGet()

    val orac_documents : Array[Recommendation] =
      search_response.getHits.getHits.map({ case (e) =>
      val item: SearchHit = e
      val id: String = item.getId

      val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

      val user_id : String = source.get("user_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val item_id : String = source.get("item_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val name : String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val generation_batch : String = source.get("generation_batch") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val generation_timestamp : Long = source.get("generation_timestamp") match {
        case Some(t) => t.asInstanceOf[Long]
        case None => 0
      }

      val score : Double = source.get("score") match {
        case Some(t) => t.asInstanceOf[Double]
        case None => 0.0
      }

      val recommendation = Recommendation(id = Option { id }, name = name, user_id = user_id, item_id = item_id,
        generation_batch = generation_batch,
        generation_timestamp = generation_timestamp, score = score)
      recommendation
    })

    val documents = if (orac_documents.nonEmpty) {
      orac_documents
    } else {
      if(forwardService.forwardEnabled(index_name) &&
        forwardService.forwardingDestinations.contains(index_name)) {
        //TODO: we take the first (head) forwarder for the index, should be improved with a preferred source setting.
        // a better method would be to pick up a random entry among those who provides recommendations i.e.: csrec
        val forwarder = forwardService.forwardingDestinations(index_name)
        val recommendations = forwarder.head._2.get_recommendations(user_id = id, size = size)
        recommendations.getOrElse(Array.empty[Recommendation])
      } else {
        Array.empty[Recommendation]
      }
    }

    val recommendations_history = documents.map(rec => {
      val access_timestamp: Option[Long] = Option{Time.getTimestampMillis}
      val recommendation_history = RecommendationHistory(id = Option.empty[String], recommendation_id = rec.id.get,
        name = rec.name, access_user_id = Option { access_user_id },
        user_id = rec.user_id, item_id = rec.item_id,
        generation_batch = rec.generation_batch,
        generation_timestamp = rec.generation_timestamp,
        access_timestamp = access_timestamp, score = rec.score)
      recommendation_history
    })

    recommendations_history.foreach(recomm => {
      recommendationHistoryService.create(index_name, access_user_id, recomm, 0)
    })

    val recommendations = Option{ Recommendations(items = documents.sortBy( - _.score)) }
    recommendations
  }

}