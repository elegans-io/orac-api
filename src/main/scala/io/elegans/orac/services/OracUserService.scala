package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.services.RecommendationService.forwardService
import io.elegans.orac.tools.Time
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.{SearchRequestBuilder, SearchResponse, SearchType}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.geo.GeoPoint
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{BoolQueryBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._

/**
  * Implements functions, eventually used by UserResource
  */
object OracUserService {
  val elasticClient: OracUserElasticClient.type = OracUserElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  private[this] def fullIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.oracUserIndexSuffix)
  }

  def create(indexName: String, document: OracUser, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
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

    val timestamp: Long = document.timestamp.getOrElse(Time.timestampMillis)
    builder.field("timestamp", timestamp)

    if (document.props.isDefined) {
      if (document.props.get.numerical.isDefined) {
        val properties = document.props.get.numerical.get
        val propertiesArray = builder.startArray("numerical_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.string.isDefined) {
        val properties = document.props.get.string.get
        val propertiesArray = builder.startArray("string_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.timestamp.isDefined) {
        val properties = document.props.get.timestamp.get
        val propertiesArray = builder.startArray("timestamp_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.geopoint.isDefined) {
        val properties = document.props.get.geopoint.get
        val propertiesArray = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopoint_value = new GeoPoint(e.value.lat, e.value.lon)
          propertiesArray.startObject.field("key", e.key).field("value", geopoint_value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.tags.isDefined) {
        val properties = document.props.get.tags.get
        val propertiesArray = builder.startArray("tag_properties")
        properties.foreach(e => {
          propertiesArray.value(e)
        })
        propertiesArray.endArray()
      }
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(fullIndexName(indexName))
      .setType(elasticClient.oracUserIndexSuffix)
      .setId(document.id)
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

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        item_type = ForwardType.orac_user,
        operation = ForwardOperationType.create, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateOracUser,
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

    val timestamp: Long = document.timestamp.getOrElse(Time.timestampMillis)
    builder.field("timestamp", timestamp)

    if (document.props.isDefined) {
      if (document.props.get.numerical.isDefined) {
        val properties = document.props.get.numerical.get
        val propertiesArray = builder.startArray("numerical_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.string.isDefined) {
        val properties = document.props.get.string.get
        val propertiesArray = builder.startArray("string_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.geopoint.isDefined) {
        val properties = document.props.get.geopoint.get
        val propertiesArray = builder.startArray("geopoint_properties")
        properties.foreach(e => {
          val geopointValue = new GeoPoint(e.value.lat, e.value.lon)
          propertiesArray.startObject.field("key", e.key).field("value", geopointValue).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.timestamp.isDefined) {
        val properties = document.props.get.timestamp.get
        val propertiesArray = builder.startArray("timestamp_properties")
        properties.foreach(e => {
          propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
        })
        propertiesArray.endArray()
      }

      if (document.props.get.tags.isDefined) {
        val properties = document.props.get.tags.get
        val propertiesArray = builder.startArray("tag_properties")
        properties.foreach(e => {
          propertiesArray.value(e)
        })
        propertiesArray.endArray()
      }
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(fullIndexName(indexName))
      .setType(elasticClient.oracUserIndexSuffix).setId(id)
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

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        item_type = ForwardType.orac_user,
        operation = ForwardOperationType.update, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(fullIndexName(indexName))
      .setType(elasticClient.oracUserIndexSuffix).setId(id).get()


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

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        item_type = ForwardType.orac_user,
        operation = ForwardOperationType.delete, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def read(indexName: String, ids: List[String]): Future[Option[OracUsers]] = {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(fullIndexName(indexName), elasticClient.oracUserIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + indexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : List[OracUser] = response.getResponses
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

      val timestamp : Option[Long] = source.get("timestamp") match {
        case Some(t) => Option{ t.asInstanceOf[Long] }
        case None => Option{0}
      }

      val numericalProperties : Option[Array[NumericalProperties]] =
        source.get("numerical_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", None.orNull).asInstanceOf[String]
              val value = x.getOrDefault("value", None.orNull).asInstanceOf[Double]
              println(NumericalProperties(key = key, value = value))
              NumericalProperties(key = key, value = value)
            }).filter(_.key =/= None.orNull).toArray
            Option { properties }
          case None => Option.empty[Array[NumericalProperties]]
        }

      val stringProperties : Option[Array[StringProperties]] =
        source.get("string_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", None.orNull)
              val value = x.getOrDefault("value", None.orNull)
              StringProperties(key = key, value = value)
            }).filter(_.key =/= None.orNull).toArray
            Option { properties }
          case None => Option.empty[Array[StringProperties]]
        }

      val timestampProperties : Option[Array[TimestampProperties]] =
        source.get("timestamp_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", None.orNull).asInstanceOf[String]
              val value = try {
                x.getOrDefault("value", None.orNull).asInstanceOf[Long]
              } catch {
                case e: Exception =>
                  0L
              }
              TimestampProperties(key = key, value = value)
            }).filter(_.key =/= None.orNull).toArray
            Option { properties }
          case None => Option.empty[Array[TimestampProperties]]
        }

      val geopointProperties : Option[Array[GeoPointProperties]] =
        source.get("geopoint_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( x => {
              val key = x.getOrDefault("key", None.orNull).asInstanceOf[String]
              val geopoint = x.getOrDefault("value", None.orNull).asInstanceOf[java.util.HashMap[String, Double]].asScala
              val value = OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
              GeoPointProperties(key = key, value = value)
            }).filter(_.key =/= None.orNull).toArray
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

      val document = OracUser(id = id, name = name, email = email, phone = phone, props = properties)
      document
    })

    Future { Option{OracUsers(items = documents)} }
  }

  def allDocuments(indexName: String, search: Option[OracUserSearch] = Option.empty,
                   keepAlive: Long = 60000): Iterator[OracUser] = {

    val qb: QueryBuilder = search match {
      case Some(document) =>
        val boolQueryBuilder = QueryBuilders.boolQuery()

        document.name match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("name", value))
          case _ => ;
        }

        document.email match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("email", value))
          case _ => ;
        }

        document.phone match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("phone", value))
          case _ => ;
        }

        document.timestamp_from match {
          case Some(value) => boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").gte(value))
          case _ => ;
        }

        document.timestamp_to match {
          case Some(value) => boolQueryBuilder.filter(QueryBuilders.rangeQuery("timestamp").lt(value))
          case _ => ;
        }

        boolQueryBuilder
      case _ =>
        QueryBuilders.matchAllQuery()
    }

    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(fullIndexName(indexName))
      .setScroll(new TimeValue(keepAlive))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually{
      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

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

        val timestamp : Option[Long] = source.get("timestamp") match {
          case Some(t) => Option{ t.asInstanceOf[Long] }
          case None => Option{0}
        }

        val numericalProperties : Option[Array[NumericalProperties]] =
          source.get("numerical_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", None.orNull).asInstanceOf[String]
                val value = x.getOrDefault("value", None.orNull).asInstanceOf[Double]
                println(NumericalProperties(key = key, value = value))
                NumericalProperties(key = key, value = value)
              }).filter(_.key =/= None.orNull).toArray
              Option { properties }
            case None => Option.empty[Array[NumericalProperties]]
          }

        val stringProperties : Option[Array[StringProperties]] =
          source.get("string_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, String]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", None.orNull)
                val value = x.getOrDefault("value", None.orNull)
                StringProperties(key = key, value = value)
              }).filter(_.key =/= None.orNull).toArray
              Option { properties }
            case None => Option.empty[Array[StringProperties]]
          }

        val timestampProperties : Option[Array[TimestampProperties]] =
          source.get("timestamp_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", None.orNull).asInstanceOf[String]
                val value = try {
                  x.getOrDefault("value", None.orNull).asInstanceOf[Long]
                } catch {
                  case e: Exception =>
                    0L
                }
                TimestampProperties(key = key, value = value)
              }).filter(_.key =/= None.orNull).toArray
              Option { properties }
            case None => Option.empty[Array[TimestampProperties]]
          }

        val geopointProperties : Option[Array[GeoPointProperties]] =
          source.get("geopoint_properties") match {
            case Some(t) =>
              val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
                .asScala.map( x => {
                val key = x.getOrDefault("key", None.orNull).asInstanceOf[String]
                val geopoint = x.getOrDefault("value", None.orNull).asInstanceOf[java.util.HashMap[String, Double]].asScala
                val value = OracGeoPoint(lat = geopoint("lat"), lon = geopoint("lon"))
                GeoPointProperties(key = key, value = value)
              }).filter(_.key =/= None.orNull).toArray
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

        val document = OracUser(id = id, name = name, email = email, phone = phone, props = properties)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile{case(_, docNonEmpty) => docNonEmpty}
      .flatMap{case (docs, _) => docs}

    iterator
  }

}
