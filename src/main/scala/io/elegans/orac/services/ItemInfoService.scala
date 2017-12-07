package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/12/17.
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
import io.elegans.orac.services.RecommendationService.forwardService
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.SearchHit

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Implements functions, eventually used by ItemInfoResource
  */
object ItemInfoService {
  val elastic_client: ItemInfoElasticClient.type = ItemInfoElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  var item_info_service = Map.empty[String, ItemInfo]
  val indexManagementService: IndexManagementService.type = IndexManagementService

  def getIndexName(index_name: String, suffix: Option[String] = None): String = {
    index_name + "." + suffix.getOrElse(elastic_client.item_info_index_suffix)
  }

  def updateItemInfoService(index_name: String): Unit = {
    if(indexManagementService.check_index_status(index_name)) {
      item_info_service = getAllDocuments(index_name).map(x => {
        (index_name + "." + x.id, x)
      }).toMap
    } else {
      log.warning("cannot update the item_info_service index not ready: " + index_name)
    }
  }

  def create(index_name: String, document: ItemInfo, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)

    val properties = document.base_fields
    val properties_array = builder.startArray("base_fields")
    properties.foreach(e => {
      properties_array.value(e)
    })
    properties_array.endArray()

    builder.field("tag_filters", document.tag_filters)
    builder.field("numerical_filters", document.numerical_filters)
    builder.field("string_filters", document.string_filters)
    builder.field("timestamp_filters", document.timestamp_filters)
    builder.field("geopoint_filters", document.geopoint_filters)

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(getIndexName(index_name))
      .setType(elastic_client.item_info_index_suffix)
      .setCreate(true)
      .setId(document.id)
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

    updateItemInfoService(index_name)
    Option {doc_result}
  }

  def update(index_name: String, id: String, document: UpdateItemInfo,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    if (document.base_fields.isDefined) {
      val properties = document.base_fields.get
      val properties_array = builder.startArray("base_fields")
      properties.foreach(e => {
        properties_array.value(e)
      })
      properties_array.endArray()
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

    val client: TransportClient = elastic_client.get_client()
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(index_name))
      .setType(elastic_client.item_info_index_suffix).setId(id)
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

    updateItemInfoService(index_name)
    Option {doc_result}
  }

  def delete(index_name: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(index_name))
      .setType(elastic_client.item_info_index_suffix).setId(id).get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName(index_name))
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception("Item : index refresh failed: (" + index_name + ")")
      }
    }

    val doc_result: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    updateItemInfoService(index_name)
    Option {doc_result}
  }

  def read(index_name: String, ids: List[String]): Future[Option[ItemInfoRecords]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName(index_name), elastic_client.item_info_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + index_name + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents: List[ItemInfo] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val base_fields : Set[String] = source.get("base_fields") match {
        case Some(t) =>
          val properties = t.asInstanceOf[java.util.ArrayList[String]]
            .asScala.toSet
          properties
        case None => Set.empty[String]
      }

      val tag_filters: String = source.get("tag_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val numerical_filters: String = source.get("numerical_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val string_filters: String = source.get("string_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val timestamp_filters: String = source.get("timestamp_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val geopoint_filters: String = source.get("geopoint_filters") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val document : ItemInfo = ItemInfo(id = id, base_fields = base_fields, tag_filters = tag_filters,
        numerical_filters = numerical_filters, string_filters = string_filters,
        timestamp_filters = timestamp_filters,
        geopoint_filters = geopoint_filters)
      document
    })

    Option{ ItemInfoRecords(items = documents) }
  }

  def getAllDocuments(index_name: String): Iterator[ItemInfo] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elastic_client.get_client()
      .prepareSearch(getIndexName(index_name))
      .setScroll(new TimeValue(60000))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually{
      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val base_fields : Set[String] = source.get("base_fields") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[String]]
              .asScala.toSet
            properties
          case None => Set.empty[String]
        }

        val tag_filters: String = source.get("tag_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val numerical_filters: String = source.get("numerical_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val string_filters: String = source.get("string_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val timestamp_filters: String = source.get("timestamp_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val geopoint_filters: String = source.get("geopoint_filters") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val document : ItemInfo = ItemInfo(id = id, base_fields = base_fields, tag_filters = tag_filters,
          numerical_filters = numerical_filters, string_filters = string_filters,
          timestamp_filters = timestamp_filters,
          geopoint_filters = geopoint_filters)
        document
      })

      scrollResp = elastic_client.get_client().prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }
}