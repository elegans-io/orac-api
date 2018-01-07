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
import io.elegans.orac.services.RecommendationService.forwardService
import org.elasticsearch.action.search.SearchResponse

import scala.concurrent.ExecutionContext.Implicits.global
import org.elasticsearch.common.geo.GeoPoint
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.search.SearchHit

/**
  * Implements functions, eventually used by ItemResource
  */
object ItemService {
  val elastic_client: ItemElasticClient.type = ItemElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  def getIndexName(index_name: String, suffix: Option[String] = None): String = {
    index_name + "." + suffix.getOrElse(elastic_client.item_index_suffix)
  }

  def create(index_name: String, document: Item, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)
    builder.field("type", document.`type`)

    document.description match {
      case Some(t) => builder.field("description", t)
      case None => ;
    }

    builder.field("name", document.name)

    if (document.properties.isDefined) {
      if (document.properties.get.numerical.isDefined) {
        val properties = document.properties.get.numerical.get
        val properties_array = builder.startArray("numerical_properties")
        properties.foreach(e => {
          properties_array.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.string.isDefined) {
        val properties = document.properties.get.string.get
        val properties_array = builder.startArray("string_properties")
        properties.foreach(e => {
          properties_array.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.geopoint.isDefined) {
        val properties = document.properties.get.geopoint.get
        val properties_array = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopoint_value = new GeoPoint(e.value.lat, e.value.lon)
          properties_array.startObject.field("key", e.key).field("value", geopoint_value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.timestamp.isDefined) {
        val properties = document.properties.get.timestamp.get
        val properties_array = builder.startArray("timestamp_properties")
        properties.foreach(e => {
          properties_array.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.tags.isDefined) {
        val properties = document.properties.get.tags.get
        val properties_array = builder.startArray("tag_properties")
        properties.foreach(e => {
          properties_array.value(e)
        })
        properties_array.endArray()
      }
    }

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(getIndexName(index_name))
      .setType(elastic_client.item_index_suffix)
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

    if(forwardService.forwardEnabled(index_name)) {
      val forward = Forward(doc_id = document.id, index = index_name,
        index_suffix = elastic_client.item_index_suffix,
        operation = "create")
      forwardService.create(document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def update(index_name: String, id: String, document: UpdateItem,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.`type` match {
      case Some(t) => builder.field("type", t)
      case None => ;
    }

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }
    document.description match {
      case Some(t) => builder.field("description", t)
      case None => ;
    }

    if (document.properties.isDefined) {
      if (document.properties.get.numerical.isDefined) {
        val properties = document.properties.get.numerical.get
        val properties_array = builder.startArray("numerical_properties")
        properties.foreach(e => {
          properties_array.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.string.isDefined) {
        val properties = document.properties.get.string.get
        val properties_array = builder.startArray("string_properties")
        properties.foreach(e => {
          properties_array.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.timestamp.isDefined) {
        val properties = document.properties.get.timestamp.get
        val properties_array = builder.startArray("timestamp_properties")
        properties.foreach(e => {
          properties_array.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.geopoint.isDefined) {
        val properties = document.properties.get.geopoint.get
        val properties_array = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopoint_value = new GeoPoint(e.value.lat, e.value.lon)
          properties_array.startObject.field("key", e.key).field("value", geopoint_value).endObject()
        })
        properties_array.endArray()
      }

      if (document.properties.get.tags.isDefined) {
        val properties = document.properties.get.tags.get
        val properties_array = builder.startArray("tag_properties")
        properties.foreach(e => {
          properties_array.value(e)
        })
        properties_array.endArray()
      }
    }

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(index_name))
      .setType(elastic_client.item_index_suffix).setId(id)
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
      val forward = Forward(doc_id = id, index = index_name,
        index_suffix = elastic_client.item_index_suffix,
        operation = "update")
      forwardService.create(document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def delete(index_name: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(index_name))
      .setType(elastic_client.item_index_suffix).setId(id).get()

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

    if(forwardService.forwardEnabled(index_name)) {
      val forward = Forward(doc_id = id, index = index_name,
        index_suffix = elastic_client.item_index_suffix,
        operation = "delete")
      forwardService.create(document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def read(index_name: String, ids: List[String]): Future[Option[Items]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName(index_name), elastic_client.item_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + index_name + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents: List[Item] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: String = source.get("name") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val `type` : String = source.get("type") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val description : Option[String] = source.get("description") match {
        case Some(t) => Option { t.asInstanceOf[String] }
        case None => Option.empty[String]
      }

      val numerical_properties : Option[Array[NumericalProperties]] =
        source.get("numerical_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val value = x.getOrDefault("value", null).asInstanceOf[Double]
              println(NumericalProperties(key = key, value = value))
              NumericalProperties(key = key, value = value)
            }).filter(_.key != null).toArray
            Option { properties }
          case None => Option.empty[Array[NumericalProperties]]
        }

      val string_properties : Option[Array[StringProperties]] =
        source.get("string_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null)
              val value = x.getOrDefault("value", null)
              StringProperties(key = key, value = value)
            }).filter(_.key != null).toArray
            Option { properties }
          case None => Option.empty[Array[StringProperties]]
        }

      val timestamp_properties : Option[Array[TimestampProperties]] =
        source.get("timestamp_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val value = x.getOrDefault("value", null).asInstanceOf[Long]
              TimestampProperties(key = key, value = value)
            }).filter(_.key != null).toArray
            Option { properties }
          case None => Option.empty[Array[TimestampProperties]]
        }

      val geopoint_properties : Option[Array[GeoPointProperties]] =
        source.get("geopoint_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val geopoint = x.getOrDefault("value", null).asInstanceOf[java.util.HashMap[String, Double]].asScala
              val value = OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
              GeoPointProperties(key = key, value = value)
            }).filter(_.key != null).toArray
            Option { properties }
          case None => Option.empty[Array[GeoPointProperties]]
        }

      val tag_properties : Option[Array[String]] = source.get("tag_properties") match {
        case Some(t) =>
          val properties = t.asInstanceOf[java.util.ArrayList[String]]
            .asScala.toArray
          Option { properties }
        case None => Option.empty[Array[String]]
      }

      val properties: Option[OracProperties] = Option { OracProperties(numerical = numerical_properties,
        string = string_properties, timestamp = timestamp_properties, geopoint = geopoint_properties,
        tags = tag_properties)
      }

      val document : Item = Item(id = id, name = name, `type` = `type`, description = description,
        properties = properties)
      document
    })

    Option{ Items(items = documents) }
  }

  def getAllDocuments(index_name: String): Iterator[Item] = {
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

        val name: String = source.get("name") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val `type` : String = source.get("type") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val description : Option[String] = source.get("description") match {
          case Some(t) => Option { t.asInstanceOf[String] }
          case None => Option.empty[String]
        }

        val numerical_properties : Option[Array[NumericalProperties]] =
          source.get("numerical_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", null).asInstanceOf[String]
                val value = x.getOrDefault("value", null).asInstanceOf[Double]
                println(NumericalProperties(key = key, value = value))
                NumericalProperties(key = key, value = value)
              }).filter(_.key != null).toArray
              Option { properties }
            case None => Option.empty[Array[NumericalProperties]]
          }

        val string_properties : Option[Array[StringProperties]] =
          source.get("string_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", null)
                val value = x.getOrDefault("value", null)
                StringProperties(key = key, value = value)
              }).filter(_.key != null).toArray
              Option { properties }
            case None => Option.empty[Array[StringProperties]]
          }

        val timestamp_properties : Option[Array[TimestampProperties]] =
          source.get("timestamp_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", null).asInstanceOf[String]
                val value = x.getOrDefault("value", null).asInstanceOf[Long]
                TimestampProperties(key = key, value = value)
              }).filter(_.key != null).toArray
              Option { properties }
            case None => Option.empty[Array[TimestampProperties]]
          }

        val geopoint_properties : Option[Array[GeoPointProperties]] =
          source.get("geopoint_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", null).asInstanceOf[String]
                val geopoint = x.getOrDefault("value", null).asInstanceOf[java.util.HashMap[String, Double]].asScala
                val value = OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
                GeoPointProperties(key = key, value = value)
              }).filter(_.key != null).toArray
              Option { properties }
            case None => Option.empty[Array[GeoPointProperties]]
          }

        val tag_properties : Option[Array[String]] = source.get("tag_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[String]]
              .asScala.toArray
            Option { properties }
          case None => Option.empty[Array[String]]
        }

        val properties: Option[OracProperties] = Option { OracProperties(numerical = numerical_properties,
          string = string_properties, timestamp = timestamp_properties, geopoint = geopoint_properties,
          tags = tag_properties)
        }

        val document : Item = Item(id = id, name = name, `type` = `type`, description = description,
          properties = properties)
        document
      })

      scrollResp = elastic_client.get_client().prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }

}