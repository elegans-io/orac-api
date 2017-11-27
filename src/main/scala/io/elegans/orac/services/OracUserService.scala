package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import io.elegans.orac.entities._
import scala.concurrent.{Future}
import scala.collection.immutable.{List, Map}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.action.delete.{DeleteResponse}
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import scala.collection.JavaConverters._
import org.elasticsearch.rest.RestStatus
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
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

    builder.field("id", document.id)

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.gender match {
      case Some(t) => builder.field("gender", t)
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

    document.birthdate match {
      case Some(t) => builder.field("birthdate", t)
      case None => ;
    }

    document.birthplace match {
      case Some(t) => builder.startObject("birthplace").field("lat", t.lat).field("lon", t.lon).endObject()
      case None => ;
    }

    document.livingplace match {
      case Some(t) => builder.startObject("livingplace").field("lat", t.lat).field("lon", t.lon).endObject()
      case None => ;
    }

    document.tags match {
      case Some(t) =>
        val properties_array = builder.startArray("tag_properties")
        document.tags.get.foreach(e => {
          properties_array.value(e)
        })
        properties_array.endArray()
      case None => ;
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

    Option {doc_result}
  }

  def update(index_name: String, id: String, document: UpdateOracUser, refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    document.gender match {
      case Some(t) => builder.field("gender", t)
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

    document.birthdate match {
      case Some(t) => builder.field("birthdate", t)
      case None => ;
    }

    document.birthplace match {
      case Some(t) => builder.startObject("birthplace").field("lat", t.lat).field("lon", t.lon).endObject()
      case None => ;
    }

    document.livingplace match {
      case Some(t) => builder.startObject("livingplace").field("lat", t.lat).field("lon", t.lon).endObject()
      case None => ;
    }

    document.tags match {
      case Some(t) =>
        val properties_array = builder.startArray("tag_properties")
        document.tags.get.foreach(e => {
          properties_array.value(e)
        })
        properties_array.endArray()
      case None => ;
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

      val gender : Option[String] = source.get("gender") match {
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

      val birthdate : Option[Long] = source.get("birthdate") match {
        case Some(t) => Option{ t.asInstanceOf[Long] }
        case None => Option.empty[Long]
      }

      val birthplace : Option[OracGeoPoint] = source.get("birthplace") match {
        case Some(t) =>
          val geopoint = t.asInstanceOf[java.util.HashMap[String, Double]].asScala
          Option {
            OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
          }
        case None => Option.empty[OracGeoPoint]
      }

      val livingplace : Option[OracGeoPoint] = source.get("livingplace") match {
        case Some(t) =>
          val geopoint = t.asInstanceOf[java.util.HashMap[String, Double]].asScala
          Option {
            OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
          }
        case None => Option.empty[OracGeoPoint]
      }

      val tag_properties : Option[List[String]] = source.get("tag_properties") match {
        case Some(t) =>
          val properties = t.asInstanceOf[java.util.ArrayList[String]]
            .asScala.toList
          Option { properties }
        case None => Option.empty[List[String]]
      }

      val document = OracUser(id = id, name = name, phone = phone, gender = gender,
        email = email, birthdate = birthdate, birthplace = birthplace, livingplace = livingplace,
        tags = tag_properties)
      document
    }) }

    Future { documents }
  }
}
