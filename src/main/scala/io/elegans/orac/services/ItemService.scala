package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.services.RecommendationService.forwardService
import io.elegans.orac.tools._
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.geo.GeoPoint
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.functionscore.RandomScoreFunctionBuilder
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._

/**
  * Implements functions, eventually used by ItemResource
  */
object ItemService {
  val elasticClient: ItemElasticClient.type = ItemElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val cronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService

  private[this] def fullIndexName(indexName: String, suffix: Option[String] = None): String = {
    indexName + "." + suffix.getOrElse(elasticClient.itemIndexSuffix)
  }

/*
  documentSearch.random.filter(identity) match {
    case Some(true) =>
      val randomBuilder = new RandomScoreFunctionBuilder().seed(RandomNumbers.getInt())
      val functionScoreQuery: QueryBuilder = QueryBuilders.functionScoreQuery(randomBuilder)
      boolQueryBuilder.must(functionScoreQuery)
    case _ => ;
  }
*/

  def create(indexName: String, document: Item, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    builder.field("id", document.id)
    builder.field("category", document.category)

    document.description match {
      case Some(t) => builder.field("description", t)
      case None => ;
    }

    builder.field("name", document.name)

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
          val geopoint_value = new GeoPoint(e.value.lat, e.value.lon)
          propertiesArray.startObject.field("key", e.key).field("value", geopoint_value).endObject()
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
        val properties_array = builder.startArray("tag_properties")
        properties.foreach(e => {
          properties_array.value(e)
        })
        properties_array.endArray()
      }
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(fullIndexName(indexName))
      .setType(elasticClient.itemIndexSuffix)
      .setCreate(true)
      .setId(document.id)
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
      val forward = Forward(doc_id = document.id, index = Some(indexName),
        item_type = ForwardType.item,
        operation = ForwardOperationType.create, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def update(indexName: String, id: String, document: UpdateItem,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.category match {
      case Some(t) => builder.field("category", t)
      case None => ;
    }

    document.name match {
      case Some(t) => builder.field("name", t)
      case None => ;
    }

    val timestamp: Long = document.timestamp.getOrElse(Time.timestampMillis)
    builder.field("timestamp", timestamp)

    document.description match {
      case Some(t) => builder.field("description", t)
      case None => ;
    }

    document.props match {
      case Some(properties) =>
        properties.numerical match {
          case Some(numericalProps) =>
            val propertiesArray = builder.startArray("numerical_properties")
            numericalProps.foreach(e => {
              propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
            })
            propertiesArray.endArray()
          case _ => ;
        }

        properties.string match {
          case Some(stringProps) =>
            val propertiesArray = builder.startArray("string_properties")
            stringProps.foreach(e => {
              propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
            })
            propertiesArray.endArray()
          case _ => ;
        }

        properties.timestamp match {
          case Some(timestampProps) =>
            val propertiesArray = builder.startArray("timestamp_properties")
            timestampProps.foreach(e => {
              propertiesArray.startObject.field("key", e.key).field("value", e.value).endObject()
            })
            propertiesArray.endArray()
          case _ => ;
        }

        properties.geopoint match {
          case Some(geopointProps) =>
            val propertiesArray = builder.startArray("geopoint_properties")
            geopointProps.foreach(e => {
              val geopointValue = new GeoPoint(e.value.lat, e.value.lon)
              propertiesArray.startObject.field("key", e.key).field("value", geopointValue).endObject()
            })
            propertiesArray.endArray()
          case _ => ;
        }

        properties.tags match {
          case Some(tagsProp) =>
            val propertiesArray = builder.startArray("tag_properties")
            tagsProp.foreach(e => {
              propertiesArray.value(e)
            })
            propertiesArray.endArray()
          case _ => ;
        }
      case _ => ;
    }

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(fullIndexName(indexName))
      .setType(elasticClient.itemIndexSuffix).setId(id)
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
        item_type = ForwardType.item,
        operation = ForwardOperationType.update, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(fullIndexName(indexName))
      .setType(elasticClient.itemIndexSuffix).setId(id).get()

    if (refresh =/= 0) {
      val refreshIndex = elasticClient.refreshIndex(fullIndexName(indexName))
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception("Item : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status =/= RestStatus.NOT_FOUND
    )

    if(forwardService.forwardEnabled(indexName)) {
      val forward = Forward(doc_id = id, index = Some(indexName),
        item_type = ForwardType.item,
        operation = ForwardOperationType.delete, retry = Option{10})
      forwardService.create(indexName = indexName, document = forward, refresh = refresh)
    }

    Option {docResult}
  }

  def read(indexName: String, ids: List[String]): Future[Option[Items]] = Future {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(fullIndexName(indexName), elasticClient.itemIndexSuffix, ids:_*)
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

      val category : String = source.get("category") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val timestamp : Option[Long] = source.get("timestamp") match {
        case Some(t) => Option{ t.asInstanceOf[Long] }
        case None => Option{0}
      }

      val description : Option[String] = source.get("description") match {
        case Some(t) => Option { t.asInstanceOf[String] }
        case None => Option.empty[String]
      }

      val numericalProperties : Option[Array[NumericalProperties]] =
        source.get("numerical_properties") match {
          case Some(t) =>
            val properties = t.asInstanceOf[java.util.ArrayList[java.util.HashMap[String, Any]]]
              .asScala.map( numericalProperty => {
              val key = numericalProperty.getOrDefault("key", None).asInstanceOf[String]
              val value = numericalProperty.getOrDefault("value", None).asInstanceOf[Double]
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

      val document : Item = Item(id = id, name = name, category = category, description = description,
        props = properties)
      document
    })

    Option{ Items(items = documents) }
  }

  def allDocuments(index_name: String, search: Option[ItemSearch] = Option.empty,
                   keepAlive: Long = 60000): Iterator[Item] = {

    val qb: QueryBuilder = search match {
      case Some(document) =>
        val boolQueryBuilder = QueryBuilders.boolQuery()

        document.name match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("name", value))
          case _ => ;
        }

        document.category match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("category", value))
          case _ => ;
        }

        document.description match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("description", value))
          case _ => ;
        }

        document.random.filter(identity) match {
          case Some(true) =>
            val randomBuilder = new RandomScoreFunctionBuilder().seed(RandomNumbers.intPos)
            val functionScoreQuery: QueryBuilder = QueryBuilders.functionScoreQuery(randomBuilder)
            boolQueryBuilder.must(functionScoreQuery)
          case _ => ;
        }

        document.random match {
          case Some(value) =>
            boolQueryBuilder.filter(QueryBuilders.termQuery("description", value))
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
      .prepareSearch(fullIndexName(index_name))
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

        val category : String = source.get("category") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val description : Option[String] = source.get("description") match {
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
                val geopoint = x.getOrDefault("value", None.orNull)
                  .asInstanceOf[java.util.HashMap[String, Double]].asScala
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

        val document : Item = Item(id = id, name = name, category = category, description = description,
          props = properties)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()
      (documents, documents.nonEmpty)
    }.takeWhile{case (_, docNonEmpty) => docNonEmpty}
      .flatMap{case (doc, _) => doc}

    iterator
  }

}