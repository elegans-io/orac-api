package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

import io.elegans.orac.entities._

import scala.collection.immutable.{List, Map}
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}

import scala.collection.JavaConverters._
import org.elasticsearch.rest.RestStatus
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities.ReconcileType
import io.elegans.orac.tools.{Checksum, Time}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryAction}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

/**
  * Implements reconciliation functions
  */

object  ReconcileService {
  val elastic_client: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService
  val recommendationService: RecommendationService.type = RecommendationService
  val cronReconcileService: CronReconcileService.type = CronReconcileService

  def getIndexName: String = {
    elastic_client.index_name + "." + elastic_client.reconcile_index_suffix
  }

  def create(document: Reconcile, index_name: String, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val timestamp: Long = document.timestamp.getOrElse(Time.getTimestampMillis)
    val id: String = document.id
      .getOrElse(Checksum.sha512(document.toString + timestamp + RandomNumbers.getLong))

    builder.field("id", id)
    builder.field("old_id", document.old_id)
    builder.field("new_id", document.new_id)
    builder.field("index", index_name)

    val reconcile_type = document.`type`
    val index_suffix = if(document.index_suffix.isDefined) {
      document.index_suffix
    } else {
      reconcile_type match {
        case ReconcileType.orac_user =>
          oracUserService.elastic_client.orac_user_index_suffix
        case _ =>
          throw new Exception(this.getClass.getCanonicalName + " : bad reconcile type value: (" + reconcile_type + ")")
      }
    }

    builder.field("index_suffix", index_suffix)
    builder.field("retry", document.retry)
    builder.field("type", reconcile_type)
    builder.field("timestamp", timestamp)

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(getIndexName)
      .setType(elastic_client.reconcile_index_suffix)
      .setId(id)
      .setCreate(true)
      .setSource(builder).get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName)
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + getIndexName + ")")
      }
    }

    val doc_result: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    cronReconcileService.reloadEventsOnce()

    Option {doc_result}
  }

  def update(id: String, document: UpdateReconcile,
             refresh: Int): Future[Option[UpdateDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    document.`type` match {
      case Some(t) => builder.field("type", t)
      case None => ;
    }

    document.index match {
      case Some(t) => builder.field("index", t)
      case None => ;
    }

    document.index_suffix match {
      case Some(t) => builder.field("index_suffix", t)
      case None => ;
    }

    document.timestamp match {
      case Some(t) => builder.field("timestamp", t)
      case None => ;
    }

    document.old_id match {
      case Some(t) => builder.field("old_id", t)
      case None => ;
    }

    document.new_id match {
      case Some(t) => builder.field("new_id", t)
      case None => ;
    }

    document.retry match {
      case Some(t) => builder.field("retry", t)
      case None => ;
    }

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName)
      .setType(elastic_client.reconcile_index_suffix).setId(id)
      .setDoc(builder)
      .get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName)
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + getIndexName + ")")
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

  def deleteAll(index_name: String): Future[Option[DeleteDocumentsResult]] = Future {
    val client: TransportClient = elastic_client.get_client()
    val qb = QueryBuilders.termQuery("index", index_name)
    val response: BulkByScrollResponse =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client).setMaxRetries(10)
        .source(getIndexName)
        .filter(qb)
        .filter(QueryBuilders.typeQuery(elastic_client.reconcile_index_suffix))
        .get()

    val deleted: Long = response.getDeleted

    val result: DeleteDocumentsResult = DeleteDocumentsResult(message = "delete", deleted = deleted)
    Option {result}
  }

  def reconcileUser(index_name: String, reconcile: Reconcile): Future[Unit] = Future {
    val old_user =
      Await.result(oracUserService.read(index_name = index_name, ids = List(reconcile.old_id)), 5.seconds)

    var new_user =
      Await.result(oracUserService.read(index_name = index_name, ids = List(reconcile.new_id)), 5.seconds)

    def existsOracUser(user: Option[OracUsers]) = user.isDefined && user.get.items.nonEmpty
    def notExistsOracUser(user: Option[OracUsers]) = user.isEmpty || user.get.items.isEmpty

    if(existsOracUser(old_user) && notExistsOracUser(new_user)) { // we can create the new user
      val old_user_data = old_user.get.items.head
      val new_user_data = OracUser(id = reconcile.new_id,
        name = old_user_data.name,
        email = old_user_data.email,
        phone = old_user_data.phone,
        properties = old_user_data.properties)
      val create_new_user =
        Await.result(oracUserService.create(index_name = index_name, document = new_user_data, refresh = 0), 5.seconds)
      if(create_new_user.isEmpty) {
        val message = "Reconciliation: cannot create the new_user(" + reconcile.new_id + ")"
        throw new Exception(message)
      }
    } else if (notExistsOracUser(old_user) && existsOracUser(new_user)) { // nothing to do
      val message = "Reconciliation: new_user is already defined, nothing to do"
      log.debug(message)
    } else if(notExistsOracUser(old_user) && notExistsOracUser(new_user)) { // some error occurred
      val message = "Reconciliation: both old_user(" + reconcile.old_id +
        ") and new_user(" + reconcile.new_id + ") does not esists"
      throw new Exception(message)
    }

    new_user =
      Await.result(oracUserService.read(index_name = index_name, ids = List(reconcile.new_id)), 5.seconds)
    if(existsOracUser(old_user) && existsOracUser(new_user)) { // we can delete the old user
      val delete_old_user =
        Await.result(oracUserService.delete(index_name = index_name, id = reconcile.old_id, 0), 5.seconds)
      if (delete_old_user.isEmpty) {
        val message = "Reconciliation: can't delete old user" + reconcile.old_id
        throw new Exception(message)
      }
    } else if(notExistsOracUser(new_user)) {
      val message = "Reconciliation: new user is empty" + reconcile.new_id
      throw new Exception(message)
    }

    val search_action = Some(UpdateAction(user_id = Some(reconcile.old_id)))
    val action_iterator = actionService.getAllDocuments(index_name = index_name, search = search_action)
    action_iterator.foreach(doc => {
      log.debug("Reconciliation of action: " + doc.id)
      val update_action = UpdateAction(user_id = Some(reconcile.new_id))
      val update_res =
        Await.result(actionService.update(doc.id.get, index_name, update_action, 0), 5.seconds)
      if(update_res.isEmpty) {
        val message = "Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") ; "
        throw new Exception(message)
      } else {
        log.debug("Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") result("+ update_res.toString + ")")
      }
    })

    val search_recommendation = Some(UpdateRecommendation(user_id = Some(reconcile.old_id)))
    val recommendation_iterator = recommendationService.getAllDocuments(index_name = index_name,
      search = search_recommendation)
    recommendation_iterator.foreach(doc => {
      log.debug("Reconciliation of recommendation: " + doc.id)
      val update_recommendation = UpdateRecommendation(user_id = Some(reconcile.new_id))
      val update_res =
        Await.result(recommendationService.update(doc.id.get, index_name, update_recommendation, 0), 5.seconds)
      if(update_res.isEmpty) {
        val message = "Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") ; "
        throw new Exception(message)
      } else {
        log.debug("Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") result("+ update_res.toString + ")")
      }
    })
  }

  def delete(id: String, index_name: String, refresh: Int): Future[Option[DeleteDocumentsResult]] = Future {
    val client: TransportClient = elastic_client.get_client()

    val qb: QueryBuilder = QueryBuilders.boolQuery()
      .must(QueryBuilders.termQuery("_id", id))
      .must(QueryBuilders.termQuery("index", index_name))

    val response: BulkByScrollResponse =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client).setMaxRetries(10)
        .source(getIndexName)
        .filter(qb)
        .filter(QueryBuilders.typeQuery(elastic_client.reconcile_index_suffix))
        .get()

    if (refresh != 0) {
      val refresh_index = elastic_client.refresh_index(getIndexName)
      if(refresh_index.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + getIndexName + ")")
      }
    }

    val deleted: Long = response.getDeleted

    val result: DeleteDocumentsResult = DeleteDocumentsResult(message = "delete", deleted = deleted)
    Option {result}
  }

  def read(ids: List[String], index_name: String): Future[Option[List[Reconcile]]] = {
    val client: TransportClient = elastic_client.get_client()
    val multiget_builder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multiget_builder.add(getIndexName, elastic_client.reconcile_index_suffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + getIndexName + ")")
    }

    val response: MultiGetResponse = multiget_builder.get()

    val documents : List[Reconcile] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists)
      .filter(_.getIndex == index_name).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val new_id: String = source.get("new_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val old_id: String = source.get("old_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val index: Option[String] = source.get("index") match {
        case Some(t) => Some(t.asInstanceOf[String])
        case None => Some("")
      }

      val index_suffix: Option[String] = source.get("index_suffix") match {
        case Some(t) => Some(t.asInstanceOf[String])
        case None => Some("")
      }

      val `type`: ReconcileType.Reconcile = source.get("type") match {
        case Some(t) => ReconcileType.getValue(t.asInstanceOf[String])
        case None => ReconcileType.unknown
      }

      val retry : Long = source.get("retry") match {
        case Some(t) => t.asInstanceOf[Integer].toLong
        case None => 0
      }

      val timestamp : Option[Long] = source.get("timestamp") match {
        case Some(t) => Option{t.asInstanceOf[Long]}
        case None => Option{0}
      }

      val document = Reconcile(id = Option{id}, new_id = new_id, old_id = old_id,
        index = index, index_suffix = index_suffix,
        `type` = `type`, retry = retry, timestamp = timestamp)
      document
    })

    Future { Option { documents } }
  }

  def getAllDocuments: Iterator[Reconcile] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elastic_client.get_client()
      .prepareSearch(getIndexName)
      .addSort("timestamp", SortOrder.ASC)
      .setScroll(new TimeValue(60000))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually {

      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val new_id: String = source.get("new_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val old_id: String = source.get("old_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val index: Option[String] = source.get("index") match {
          case Some(t) => Some(t.asInstanceOf[String])
          case None => Some("")
        }

        val index_suffix: Option[String] = source.get("index_suffix") match {
          case Some(t) => Some(t.asInstanceOf[String])
          case None => Some("")
        }

        val `type`: ReconcileType.Reconcile = source.get("type") match {
          case Some(t) => ReconcileType.getValue(t.asInstanceOf[String])
          case None => ReconcileType.unknown
        }

        val retry : Long = source.get("retry") match {
          case Some(t) => t.asInstanceOf[Integer].toLong
          case None => 0
        }

        val timestamp : Option[Long] = source.get("timestamp") match {
          case Some(t) => Option{t.asInstanceOf[Long]}
          case None => Option{0}
        }

        val document = Reconcile(id = Option{id}, new_id = new_id, old_id = old_id,
          index = index, index_suffix = index_suffix,
          `type` = `type`, retry = retry, timestamp = timestamp)
        document
      })

      scrollResp = elastic_client.get_client().prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(60000)).execute().actionGet()

      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }

  def read_all(index_name: String): Future[Option[List[Reconcile]]] = {
    Future { Option { getAllDocuments.toList } }
  }

}
