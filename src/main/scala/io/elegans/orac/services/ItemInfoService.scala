package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
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

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._

/**
  * Implements functions, eventually used by ItemInfoResource
  */
object ItemInfoService {
  val elasticClient: ItemInfoElasticClient.type = ItemInfoElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  var itemInfoService = Map.empty[String, ItemInfo]
  val indexManagementService: IndexManagementService.type = IndexManagementService

  private[this] def fullIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.itemInfoIndexSuffix)
  }

  def updateItemInfoService(indexName: String): Unit = {
    if(indexManagementService.checkIndexStatus(indexName)) {
      itemInfoService = allDocuments(indexName).map(x => {
        (indexName + "." + x.id, x)
      }).toMap
    } else {
      log.warning("cannot update the item_info_service index not ready: " + indexName)
    }
  }

  def create(indexName: String, document: ItemInfo, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)

    val properties = document.base_fields
    val propertiesArray = builder.startArray("base_fields")
    properties.foreach(e => {
      propertiesArray.value(e)
    })
    propertiesArray.endArray()

    builder.field("tag_filters", document.tag_filters)
    builder.field("numerical_filters", document.numerical_filters)
    builder.field("string_filters", document.string_filters)
    builder.field("timestamp_filters", document.timestamp_filters)
    builder.field("geopoint_filters", document.geopoint_filters)

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(fullIndexName(indexName))
      .setType(elasticClient.itemInfoIndexSuffix)
      .setCreate(true)
      .setId(document.id)
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

    updateItemInfoService(indexName)
    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateItemInfo,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    if (document.base_fields.isDefined) {
      val properties = document.base_fields.get
      val propertiesArray = builder.startArray("base_fields")
      properties.foreach(e => {
        propertiesArray.value(e)
      })
      propertiesArray.endArray()
    }

    if(document.tag_filters.isDefined) {
      builder.field("tag_filters", document.tag_filters)
    }

    if(document.numerical_filters.isDefined) {
      builder.field("numerical_filters", document.numerical_filters)
    }

    if(document.string_filters.isDefined) {
      builder.field("string_filters", document.string_filters)
    }

    if(document.timestamp_filters.isDefined) {
      builder.field("timestamp_filters", document.timestamp_filters)
    }

    if(document.geopoint_filters.isDefined) {
      builder.field("geopoint_filters", document.geopoint_filters)
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(fullIndexName(indexName))
      .setType(elasticClient.itemInfoIndexSuffix).setId(id)
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

    updateItemInfoService(indexName)
    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(fullIndexName(indexName))
      .setType(elasticClient.itemInfoIndexSuffix).setId(id).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception("Item : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status =/= RestStatus.NOT_FOUND
    )

    updateItemInfoService(indexName)
    Option {docResult}
  }

  def read(indexName: String, ids: List[String]): Future[Option[ItemInfoRecords]] = Future {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(fullIndexName(indexName), elasticClient.itemInfoIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + indexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents: List[ItemInfo] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val baseFields : Set[String] = source.get("base_fields") match {
        case Some(t) =>
          val properties = t.asInstanceOf[java.util.ArrayList[String]]
            .asScala.toSet
          properties
        case None => Set.empty[String]
      }

      val tagFilters: String = source.get("tag_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val numericalFilters: String = source.get("numerical_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val stringFilters: String = source.get("string_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val timestampFilters: String = source.get("timestamp_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val geopointFilters: String = source.get("geopoint_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val document : ItemInfo = ItemInfo(id = id, base_fields = baseFields, tag_filters = tagFilters,
        numerical_filters = numericalFilters, string_filters = stringFilters,
        timestamp_filters = timestampFilters,
        geopoint_filters = geopointFilters)
      document
    })

    Option{ ItemInfoRecords(items = documents) }
  }

  def allDocuments(indexName: String): Iterator[ItemInfo] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(fullIndexName(indexName))
      .setScroll(new TimeValue(60000))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually{
      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val baseFields : Set[String] = source.get("base_fields") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[String]]
              .asScala.toSet
            properties
          case None => Set.empty[String]
        }

        val tagFilters: String = source.get("tag_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val numericalFilters: String = source.get("numerical_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val stringFilters: String = source.get("string_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val timestampFilters: String = source.get("timestamp_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val geopointFilters: String = source.get("geopoint_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val document : ItemInfo = ItemInfo(id = id, base_fields = baseFields, tag_filters = tagFilters,
          numerical_filters = numericalFilters, string_filters = stringFilters,
          timestamp_filters = timestampFilters,
          geopoint_filters = geopointFilters)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile{case(_, docNonEmpty) => docNonEmpty}.flatMap{case(docs, _) => docs}

    iterator
  }
}