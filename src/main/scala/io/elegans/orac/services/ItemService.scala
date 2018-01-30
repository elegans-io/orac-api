package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.services.RecommendationService.forwardService
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.geo.GeoPoint
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

/**
  * Implements functions, eventually used by ItemResource
  */
object ItemService {
  val elasticClient: ItemElasticClient.type = ItemElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  def getIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.itemIndexSuffix)
  }

  def create(indexName: String, document: Item, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
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
        val propertiesArray = builder.startArray("numerical_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.string.isDefined) {
        val properties = document.properties.get.string.get
        val propertiesArray = builder.startArray("string_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.geopoint.isDefined) {
        val properties = document.properties.get.geopoint.get
        val propertiesArray = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopoint_value = new GeoPoint(e.value.lat, e.value.lon)
          propertiesArray.startObject.field("key", e.key).field("value", geopoint_value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.timestamp.isDefined) {
        val properties = document.properties.get.timestamp.get
        val propertiesArray = builder.startArray("timestamp_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
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

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(getIndexName(indexName))
      .setType(elasticClient.itemIndexSuffix)
      .setCreate(true)
      .setId(document.id)
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

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = document.id, index = Some(indexName),
        `type` = ForwardType.item,
        operation = ForwardOperationType.create, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateItem,
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
        val propertiesArray = builder.startArray("numerical_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.string.isDefined) {
        val properties = document.properties.get.string.get
        val propertiesArray = builder.startArray("string_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.timestamp.isDefined) {
        val properties = document.properties.get.timestamp.get
        val propertiesArray = builder.startArray("timestamp_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.geopoint.isDefined) {
        val properties = document.properties.get.geopoint.get
        val propertiesArray = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopointValue = new GeoPoint(e.value.lat, e.value.lon)
          propertiesArray.startObject.field("key", e.key).field("value", geopointValue).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.properties.get.tags.isDefined) {
        val properties = document.properties.get.tags.get
        val propertiesArray = builder.startArray("tag_properties")
        properties.foreach(e => {
          propertiesArray.value(e)
        })
        propertiesArray.endArray()
      }
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName(indexName))
      .setType(elasticClient.itemIndexSuffix).setId(id)
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

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        `type` = ForwardType.item,
        operation = ForwardOperationType.update, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName(indexName))
      .setType(elasticClient.itemIndexSuffix).setId(id).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception("Item : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        `type` = ForwardType.item,
        operation = ForwardOperationType.delete, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def read(indexName: String, ids: List[String]): Future[Option[Items]] = Future {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(getIndexName(indexName), elasticClient.itemIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + indexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

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

      val numericalProperties : Option[Array[NumericalProperties]] =
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

      val stringProperties : Option[Array[StringProperties]] =
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

      val timestampProperties : Option[Array[TimestampProperties]] =
        source.get("timestamp_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", null).asInstanceOf[String]
              val value = try {
                x.getOrDefault("value", null).asInstanceOf[Long]
              } catch {
                case e: Exception =>
                  0L
              }
              TimestampProperties(key = key, value = value)
            }).filter(_.key != null).toArray
            Option { properties }
          case None => Option.empty[Array[TimestampProperties]]
        }

      val geopointProperties : Option[Array[GeoPointProperties]] =
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

      val tagProperties : Option[Array[String]] = source.get("tag_properties") match {
        case Some(t) =>
          val properties = t.asInstanceOf[java.util.ArrayList[String]]
            .asScala.toArray
          Option { properties }
        case None => Option.empty[Array[String]]
      }

      val properties: Option[OracProperties] = Option { OracProperties(numerical = numericalProperties,
        string = stringProperties, timestamp = timestampProperties, geopoint = geopointProperties,
        tags = tagProperties)
      }

      val document : Item = Item(id = id, name = name, `type` = `type`, description = description,
        properties = properties)
      document
    })

    Option{ Items(items = documents) }
  }

  def getAllDocuments(index_name: String, keepAlive: Long = 60000): Iterator[Item] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(getIndexName(index_name))
      .setScroll(new TimeValue(keepAlive))
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

        val numericalProperties : Option[Array[NumericalProperties]] =
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

        val stringProperties : Option[Array[StringProperties]] =
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

        val timestampProperties : Option[Array[TimestampProperties]] =
          source.get("timestamp_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", null).asInstanceOf[String]
                val value = try {
                  x.getOrDefault("value", null).asInstanceOf[Long]
                } catch {
                  case e: Exception =>
                    0L
                }
                TimestampProperties(key = key, value = value)
              }).filter(_.key != null).toArray
              Option { properties }
            case None => Option.empty[Array[TimestampProperties]]
          }

        val geopointProperties : Option[Array[GeoPointProperties]] =
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

        val tagProperties : Option[Array[String]] = source.get("tag_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[String]]
              .asScala.toArray
            Option { properties }
          case None => Option.empty[Array[String]]
        }

        val properties: Option[OracProperties] = Option { OracProperties(numerical = numericalProperties,
          string = stringProperties, timestamp = timestampProperties, geopoint = geopointProperties,
          tags = tagProperties)
        }

        val document : Item = Item(id = id, name = name, `type` = `type`, description = description,
          properties = properties)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }

}