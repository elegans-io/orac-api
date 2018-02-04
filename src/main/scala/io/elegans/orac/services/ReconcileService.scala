package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities.{ReconcileType, _}
import io.elegans.orac.tools.{Checksum, Time}
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryAction}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.sort.SortOrder

import scala.collection.JavaConverters._
import scala.collection.immutable.{List, Map}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

/**
  * Implements reconciliation functions
  */

object  ReconcileService {
  val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService
  val recommendationService: RecommendationService.type = RecommendationService
  val cronReconcileService: CronReconcileService.type = CronReconcileService

  def getIndexName: String = {
    elasticClient.indexName + "." + elasticClient.reconcileIndexSuffix
  }

  def create(document: Reconcile, indexName: String, refresh: Int): Future[Option[IndexDocumentResult]] = Future {
    val builder : XContentBuilder = jsonBuilder().startObject()

    val timestamp: Long = document.timestamp.getOrElse(Time.getTimestampMillis)
    val id: String = document.id
      .getOrElse(Checksum.sha512(document.toString + timestamp + RandomNumbers.getLong))

    builder.field("id", id)
    builder.field("old_id", document.old_id)
    builder.field("new_id", document.new_id)
    builder.field("index", indexName)

    val reconcile_type = document.`type`
    builder.field("type", reconcile_type)

    builder.field("retry", document.retry)
    builder.field("timestamp", timestamp)

    builder.endObject()

    val client: TransportClient = elasticClient.getClient
    val response = client.prepareIndex().setIndex(getIndexName)
      .setType(elasticClient.reconcileIndexSuffix)
      .setId(id)
      .setCreate(true)
      .setSource(builder).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName)
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + getIndexName + ")")
      }
    }

    val docResult: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    cronReconcileService.reloadEventsOnce()

    Option {docResult}
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

    val client: TransportClient = elasticClient.getClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(getIndexName)
      .setType(elasticClient.reconcileIndexSuffix).setId(id)
      .setDoc(builder)
      .get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName)
      if(refreshIndex.failed_shards_n > 0) {
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

  def deleteAll(indexName: String): Future[Option[DeleteDocumentsResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val qb = QueryBuilders.termQuery("index", indexName)
    val response: BulkByScrollResponse =
      DeleteByQueryAction.INSTANCE.newRequestBuilder(client).setMaxRetries(10)
        .source(getIndexName)
        .filter(qb)
        .filter(QueryBuilders.typeQuery(elasticClient.reconcileIndexSuffix))
        .get()

    val deleted: Long = response.getDeleted

    val result: DeleteDocumentsResult = DeleteDocumentsResult(message = "delete", deleted = deleted)
    Option {result}
  }

  def reconcileUser(indexName: String, reconcile: Reconcile): Future[Unit] = Future {
    val oldUser =
      Await.result(oracUserService.read(indexName = indexName, ids = List(reconcile.old_id)), 5.seconds)

    val newUser =
      Await.result(oracUserService.read(indexName = indexName, ids = List(reconcile.new_id)), 5.seconds)

    def existsOracUser(user: Option[OracUsers]) = user.isDefined && user.get.items.nonEmpty
    def notExistsOracUser(user: Option[OracUsers]) = user.isEmpty || user.get.items.isEmpty

    if(existsOracUser(oldUser) && notExistsOracUser(newUser)) { // we can create the new user
      val oldUserData = oldUser.get.items.head
      val newUserData = OracUser(id = reconcile.new_id,
        name = oldUserData.name,
        email = oldUserData.email,
        phone = oldUserData.phone,
        properties = oldUserData.properties)
      val createNewUser =
        Await.result(oracUserService.create(indexName = indexName, document = newUserData, refresh = 0), 5.seconds)
      if(createNewUser.isEmpty) {
        val message = "Reconciliation: cannot create the new_user(" + reconcile.new_id + ")"
        throw new Exception(message)
      }
    } else if (notExistsOracUser(oldUser) && existsOracUser(newUser)) { // nothing to do
      val message = "Reconciliation: new_user is already defined, nothing to do"
      log.debug(message)
    } else if(notExistsOracUser(oldUser) && notExistsOracUser(newUser)) { // some error occurred
      val message = "Reconciliation: both old_user(" + reconcile.old_id + ") and new_user(" +
        reconcile.new_id + ") does not exists"
      throw new Exception(message)
    }

    val newUserJustFetched =
      Await.result(oracUserService.read(indexName = indexName, ids = List(reconcile.new_id)), 5.seconds)
    if(notExistsOracUser(newUserJustFetched)) {
      val message = "Reconciliation: new user is empty" + reconcile.new_id
      throw new Exception(message)
    }

    val searchAction = Some(UpdateAction(user_id = Some(reconcile.old_id)))
    val actionIterator = actionService.getAllDocuments(indexName = indexName, search = searchAction)
    actionIterator.foreach(doc => {
      log.debug("Reconciliation of action: " + doc.id)
      val updateAction = UpdateAction(user_id = Some(reconcile.new_id))
      val updateRes =
        Await.result(actionService.update(indexName, doc.id.get, updateAction, 0), 5.seconds)
      if(updateRes.isEmpty) {
        val message = "Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") ; "
        throw new Exception(message)
      } else {
        log.debug("Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") result("+ updateRes.toString + ")")
      }
    })

    val searchRecommendation = Some(UpdateRecommendation(user_id = Some(reconcile.old_id)))
    val recommendationIterator = recommendationService.getAllDocuments(indexName = indexName,
      search = searchRecommendation)
    recommendationIterator.foreach(doc => {
      log.debug("Reconciliation of recommendation: " + doc.id)
      val updateRecommendation = UpdateRecommendation(user_id = Some(reconcile.new_id))
      val updateRes =
        Await.result(recommendationService.update(indexName, doc.id.get, updateRecommendation, 0), 5.seconds)
      if(updateRes.isEmpty) {
        val message = "Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") ; "
        throw new Exception(message)
      } else {
        log.debug("Reconciliation: reconcile_id(" + reconcile.id + ") " +
          "type(" + reconcile.`type` + ") document_id(" + doc.id + ") result("+ updateRes.toString + ")")
      }
    })

    if(existsOracUser(oldUser) && existsOracUser(newUserJustFetched)) { // we can delete the old user
      val deleteOldUser =
        Await.result(oracUserService.delete(indexName = indexName, id = reconcile.old_id, 0), 5.seconds)
      if (deleteOldUser.isEmpty) {
        val message = "Reconciliation: can't delete old user" + reconcile.old_id
        throw new Exception(message)
      }
    }
  }

  def delete(indexName: String, id: String, refresh: Int): Future[Option[DeleteDocumentResult]] = Future {
    val client: TransportClient = elasticClient.getClient
    val response: DeleteResponse = client.prepareDelete().setIndex(getIndexName)
      .setType(elasticClient.reconcileIndexSuffix).setId(id).get()

    if (refresh != 0) {
      val refreshIndex = elasticClient.refreshIndex(getIndexName)
      if(refreshIndex.failed_shards_n > 0) {
        throw new Exception(this.getClass.getCanonicalName + " : index refresh failed: (" + indexName + ")")
      }
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    log.debug("Delete reconcile item: " + id)
    Option {docResult}
  }

  def read(ids: List[String], index_name: String): Future[Option[List[Reconcile]]] = {
    val client: TransportClient = elasticClient.getClient
    val multigetBuilder: MultiGetRequestBuilder = client.prepareMultiGet()

    if (ids.nonEmpty) {
      multigetBuilder.add(getIndexName, elasticClient.reconcileIndexSuffix, ids:_*)
    } else {
      throw new Exception(this.getClass.getCanonicalName + " : ids list is empty: (" + getIndexName + ")")
    }

    val response: MultiGetResponse = multigetBuilder.get()

    val documents : List[Reconcile] = response.getResponses
      .toList.filter((p: MultiGetItemResponse) => p.getResponse.isExists)
      .filter(_.getIndex == index_name).map( { case(e) =>

      val item: GetResponse = e.getResponse

      val id : String = item.getId

      val source : Map[String, Any] = item.getSource.asScala.toMap

      val newId: String = source.get("new_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val oldId: String = source.get("old_id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val index: Option[String] = source.get("index") match {
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

      val document = Reconcile(id = Option{id}, new_id = newId, old_id = oldId,
        index = index, `type` = `type`, retry = retry, timestamp = timestamp)
      document
    })

    Future { Option { documents } }
  }

  def getAllDocuments(keepAlive: Long = 60000): Iterator[Reconcile] = {
    val qb: QueryBuilder = QueryBuilders.matchAllQuery()
    var scrollResp: SearchResponse = elasticClient.getClient
      .prepareSearch(getIndexName)
      .addSort("timestamp", SortOrder.ASC)
      .setScroll(new TimeValue(keepAlive))
      .setQuery(qb)
      .setSize(100).get()

    val iterator = Iterator.continually {

      val documents = scrollResp.getHits.getHits.toList.map( { case(e) =>
        val item: SearchHit = e

        val id : String = item.getId

        val source : Map[String, Any] = item.getSourceAsMap.asScala.toMap

        val newId: String = source.get("new_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val oldId: String = source.get("old_id") match {
          case Some(t) => t.asInstanceOf[String]
          case None => ""
        }

        val index: Option[String] = source.get("index") match {
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

        val document = Reconcile(id = Option{id}, new_id = newId, old_id = oldId,
          index = index, `type` = `type`, retry = retry, timestamp = timestamp)
        document
      })

      scrollResp = elasticClient.getClient.prepareSearchScroll(scrollResp.getScrollId)
        .setScroll(new TimeValue(keepAlive)).execute().actionGet()

      (documents, documents.nonEmpty)
    }.takeWhile(_._2).map(_._1).flatten

    iterator
  }

  def readAll(indexName: String): Future[Option[List[Reconcile]]] = {
    Future { Option { getAllDocuments().toList } }
  }

}
