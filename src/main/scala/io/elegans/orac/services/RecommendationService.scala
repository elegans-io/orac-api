package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.tools.{Checksum, Time, RandomNumbers}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._
import org.elasticsearch.index.reindex.BulkByScrollResponse
import org.elasticsearch.index.reindex.DeleteByQueryAction

/**
  * Implements functions, eventually used by RecommendationResource
  */
object RecommendationService {
  val elasticClient: RecommendationElasticClient.type = RecommendationElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val recommendationHistoryService: RecommendationHistoryService.type = RecommendationHistoryService
  val forwardService: ForwardService.type = ForwardService

  private[this] def fullIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.recommendationIndexSuffix)
  }

  def create(indexName: String, document: Recommendation,
             refresh: Int): Future[Option[IndexDocumentResult]] = Future {
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

    if (refresh =/= 0) {
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

  def update(indexName: String, id: String, document: UpdateRecommendation,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
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

    if (refresh =/= 0) {
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

    if (refresh =/= 0) {
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

  def read(indexName: String, accessUserId: String,
           ids: List[String]): Future[Option[Recommendations]] = Future {
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

      val recommendation = Recommendation(id = Option { id }, name = name, algorithm = algorithm,
        user_id = userId, item_id = itemId,
        generation_batch = generationBatch,
        generation_timestamp = generationTimestamp, score = score)

      val accessTimestamp: Option[Long] = Option{ Time.timestampMillis }

      val recommendationHistory = RecommendationHistory(id = Option.empty[String], recommendation_id = id,
        name = name, algorithm = algorithm, access_user_id = Option { accessUserId },
        user_id = userId, item_id = itemId,
        generation_batch = generationBatch,
        generation_timestamp = generationTimestamp,
        access_timestamp = accessTimestamp, score = score)

      (recommendation, recommendationHistory)
    })

    documents.map(_._2).foreach(recomm => {
      recommendationHistoryService.create(indexName, accessUserId, recomm, 0)
    })

    val recommendations = Option{ Recommendations(items = documents.map(_._1).sortBy(- _.score)) }
    recommendations
  }

  def deleteDocumentsByQuery(indexName: String, from: Option[Long],
                      to: Option[Long]): Future[Option[DeleteDocumentsResult]] = Future {
    val qb: QueryBuilder = if(from.isEmpty && to.isEmpty) {
      QueryBuilders.matchAllQuery()
    } else {
      val boolQueryBuilder = QueryBuilders.boolQuery()
      from match {
        case Some(value) => boolQueryBuilder.filter(QueryBuilders.rangeQuery("generation_timestamp").gte(value))
        case _ => ;
      }

      to match {
        case Some(value) => boolQueryBuilder.filter(QueryBuilders.rangeQuery("generation_timestamp").lt(value))
        case _ => ;
      }
      boolQueryBuilder
    }

    val response: BulkByScrollResponse =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(elasticClient.getClient).setMaxRetries(10)
        .source(fullIndexName(indexName))
        .filter(qb)
        .get()

    val deleted: Long = response.getDeleted

    val result: DeleteDocumentsResult = DeleteDocumentsResult(message = "delete", deleted = deleted)
    Option {result}
  }


  def allDocuments(indexName: String,
                   search: Option[UpdateRecommendation] = Option.empty): Iterator[Recommendation] = {
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
      if (document.algorithm.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("algorithm", document.algorithm.get))
      if (document.generation_batch.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("generation_batch", document.generation_batch.get))
      if (document.generation_timestamp.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("generation_timestamp", document.generation_timestamp.get))
      if (document.score.isDefined)
        boolQueryBuilder.filter(QueryBuilders.termQuery("score", document.score.get))
      boolQueryBuilder
    }

    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(fullIndexName(indexName))
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

        val generationTimestamp : Long = source.get("generation_timestamp") match {
          case Some(t) => t.asInstanceOf[Long]
          case None => 0
        }

        val score : Double = source.get("score") match {
          case Some(t) => t.asInstanceOf[Double]
          case None => 0.0
        }

        val generationBatch : String = source.get("generation_batch") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val document : Recommendation = Recommendation(id = Option { id }, name = name,
          algorithm = algorithm, user_id = userId,
          item_id = itemId, generation_timestamp = generationTimestamp, score = score,
          generation_batch = generationBatch)

        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile{case(_, docNonEmpty) => docNonEmpty}.flatMap{case(docs, _) => docs}

    iterator
  }

  def getRecommendationsByUser(indexName: String, access_user_id: String, id: String,
                               from: Int = 0, size: Int = 10): Future[Option[Recommendations]] = Future {
    val client: TransportClient = elasticClient.getClient
    val searchBuilder : SearchRequestBuilder = client.prepareSearch(fullIndexName(indexName))
      .setTypes(elasticClient.recommendationIndexSuffix)
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)

    val boolQueryBuilder : BoolQueryBuilder = QueryBuilders.boolQuery()
    boolQueryBuilder.filter(QueryBuilders.termQuery("user_id", id))

    searchBuilder.setQuery(boolQueryBuilder)

    searchBuilder.addSort("score", SortOrder.DESC)

    val searchResponse : SearchResponse = searchBuilder
      .setFrom(from).setSize(size)
      .execute()
      .actionGet()

    val oracDocuments : Array[Recommendation] =
      searchResponse.getHits.getHits.map({ case (e) =>
        val item: SearchHit = e
        val id: String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val userId : String = source.get("user_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val itemId : String = source.get("item_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val name : String = source.get("name") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val algorithm : String = source.get("algorithm") match {
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

        val recommendation = Recommendation(id = Option { id }, name = name, algorithm = algorithm,
          user_id = userId, item_id = itemId,
          generation_batch = generationBatch,
          generation_timestamp = generationTimestamp, score = score)
        recommendation
      })

    val documents = if (oracDocuments.nonEmpty) {
      oracDocuments
    } else {
      if(forwardService.forwardEnabled(indexName) &&
        forwardService.forwardingDestinations.contains(indexName)) {
        //TODO: we take the first (head) forwarder for the index, should be improved with a preferred source setting.
        // a better method would be to pick up a random entry among those who provides recommendations i.e.: csrec
        val forwarder = forwardService.forwardingDestinations(indexName)
        val recommendations = forwarder.head._2.getRecommendations(userId = id, size = size)
        recommendations.getOrElse(Array.empty[Recommendation])
      } else {
        Array.empty[Recommendation]
      }
    }

    val recommendationsHistory = documents.map(rec => {
      val accessTimestamp: Option[Long] = Option{Time.timestampMillis}
      val recommendationHistory = RecommendationHistory(id = Option.empty[String],
        recommendation_id = rec.id.get,
        name = rec.name, access_user_id = Option { access_user_id },
        algorithm = rec.algorithm,
        user_id = rec.user_id, item_id = rec.item_id,
        generation_batch = rec.generation_batch,
        generation_timestamp = rec.generation_timestamp,
        access_timestamp = accessTimestamp, score = rec.score)
      recommendationHistory
    })

    recommendationsHistory.foreach(recomm => {
      recommendationHistoryService.create(indexName, access_user_id, recomm, 0)
    })

    val recommendations = Option{ Recommendations(items = documents.sortBy( - _.score)) }
    recommendations
  }

}