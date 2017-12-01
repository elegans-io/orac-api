package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
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
import io.elegans.orac.services.ActionService.elastic_client
import io.elegans.orac.services.RecommendationService.forwardService
import org.elasticsearch.common.geo.GeoPoint

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Implements functions, eventually used by UserResource
  */
object OracUserService {
  val elastic_client = OracUserElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  def getIndexName(index_name: String, suffix: Option[String] = None): String = {
    index_name + "." + suffix.getOrElse(elastic_client.orac_user_index_suffix)
  }

  def create(index_name: String, document: OracUser, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val id = document.id
    builder.field("id", id)

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.phone match {
      case Some(t) => builder.field("phone", t)
      case None => ;
    }

    document.email match {
      case Some(t) => builder.field("email", t)
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
          val geopoint =
            properties_array.startObject.field("key", e.key)
              .field("value", geopoint_value).endObject()
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
      .setType(elastic_client.orac_user_index_suffix)
      .setId(document.id)
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

    if(forwardService.forwardEnabled) {
      val forward = Forward(id = id, index = index_name,
        index_suffix = elastic_client.orac_user_index_suffix,
        operation = "create")
      forwardService.create(document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def update(index_name: String, id: String, document: UpdateOracUser,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.email match {
      case Some(t) => builder.field("email", t)
      case None => ;
    }

    document.phone match {
      case Some(t) => builder.field("phone", t)
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

      if (document.properties.get.geopoint.isDefined) {
        val properties = document.properties.get.geopoint.get
        val properties_array = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopoint_value = new GeoPoint(e.value.lat, e.value.lon)
          val geopoint =
            properties_array.startObject.field("key", e.key)
              .field("value", geopoint_value).endObject()
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
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(index_name))
      .setType(elastic_client.orac_user_index_suffix).setId(id)
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

    if(forwardService.forwardEnabled) {
      val forward = Forward(id = id, index = index_name,
        index_suffix = elastic_client.orac_user_index_suffix,
        operation = "update")
      forwardService.create(document = forward, refresh = refresh)
    }

    Option {doc_result}
  }

  def delete(index_name: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(index_name))
      .setType(elastic_client.orac_user_index_suffix).setId(id).get()


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

    if(forwardService.forwardEnabled) {
      val forward = Forward(id = id, index = index_name,
        index_suffix = elastic_client.orac_user_index_suffix,
        operation = "delete")
      forwardService.create(document = forward, refresh = refresh)
    }
    
    Option {doc_result}
  }

  def read(index_name: String, ids: List[String]): Future[Option[List[OracUser]]] = {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName(index_name), elastic_client.orac_user_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + index_name + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents : Option[List[OracUser]] = Option { response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val name: Option[String] = source.get("name") match {
        case Some(t) => Option{ t.asInstanceOf[String] }
        case None => Option.empty[String]
      }

      val email : Option[String] = source.get("email") match {
        case Some(t) => Option { t.asInstanceOf[String] }
        case None => Option.empty[String]
      }

      val phone : Option[String] = source.get("phone") match {
        case Some(t) => Option { t.asInstanceOf[String] }
        case None => Option.empty[String]
      }

      val numerical_properties : Option[List[NumericalProperties]] =
        source.get("numerical_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val value = x.getOrDefault("value", null).asInstanceOf[Double]
              println(NumericalProperties(key = key, value = value))
              NumericalProperties(key = key, value = value)
            }).filter(_.key != null).toList
            Option { properties }
          case None => Option.empty[List[NumericalProperties]]
        }

      val string_properties : Option[List[StringProperties]] =
        source.get("string_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null)
              val value = x.getOrDefault("value", null)
              StringProperties(key = key, value = value)
            }).filter(_.key != null).toList
            Option { properties }
          case None => Option.empty[List[StringProperties]]
        }

      val timestamp_properties : Option[List[TimestampProperties]] =
        source.get("timestamp_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val value = x.getOrDefault("value", null).asInstanceOf[Long]
              TimestampProperties(key = key, value = value)
            }).filter(_.key != null).toList
            Option { properties }
          case None => Option.empty[List[TimestampProperties]]
        }

      val geopoint_properties : Option[List[GeoPointProperties]] =
        source.get("geopoint_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val geopoint = x.getOrDefault("value", null).asInstanceOf[java.util.HashMap[String, Double]].asScala
              val value = OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
              GeoPointProperties(key = key, value = value)
            }).filter(_.key != null).toList
            Option { properties }
          case None => Option.empty[List[GeoPointProperties]]
        }

      val tag_properties : Option[List[String]] = source.get("tag_properties") match {
        case Some(t) =>
          val properties = t.asInstanceOf[java.util.ArrayList[String]]
            .asScala.toList
          Option { properties }
        case None => Option.empty[List[String]]
      }

      val properties: Option[OracProperties] = Option { OracProperties(numerical = numerical_properties,
        string = string_properties, timestamp = timestamp_properties, geopoint = geopoint_properties,
        tags = tag_properties)
      }

      val document = OracUser(id = id, name = name, email = email, phone = phone, properties = properties)
      document
    }) }

    Future { documents }
  }
}
