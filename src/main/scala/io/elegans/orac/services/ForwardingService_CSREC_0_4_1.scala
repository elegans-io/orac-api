package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import akka.actor.ActorSystem
import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.{Marshal, Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{ContentType, HttpEntity, MediaTypes, _}
import akka.http.scaladsl.settings.ConnectionPoolSettings
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.serializers.JsonSupport
import io.elegans.orac.tools._
import spray.json.{SerializationException, _}

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

class ForwardingService_CSREC_0_4_1(forwardingDestination: ForwardingDestination)
  extends AbstractForwardingImplService with JsonSupport {
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val httpHeader: immutable.Seq[HttpHeader] = immutable.Seq(RawHeader("application", "json"))
  val itemService: ItemService.type = ItemService
  val itemInfoService: ItemInfoService.type = ItemInfoService

  implicit val system: ActorSystem = OracActorSystem.system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  type CsrecItemType = Map[String, Any]
  type CsrecItemsArray = Array[CsrecItemType]
  implicit val mapMarshaller: ToEntityMarshaller[CsrecItemsArray] = Marshaller.opaque { obj =>
    val items = obj.map(item => {
      item.map(x => {
        val value = x._2 match {
          case v: String =>
            v.asInstanceOf[String].toJson
          case v: Array[String] =>
            v.asInstanceOf[Array[String]].toJson
          case v: Double =>
            v.asInstanceOf[Double].toJson
          case v =>
            throw new SerializationException("error serializing Array[Map[String, Any]]: " + v)
        }
        (x._1.toJson, value.toJson)
      }).toJson
    }).toJson
    HttpEntity(ContentType(MediaTypes.`application/json`), items.toString())
  }

  def executeHttpRequest(uri: String,
                         method: HttpMethod,
                         request_entity: Option[RequestEntity] = Option.empty[RequestEntity]):
  Future[HttpResponse] = {
    // the duration must be high for csrec
    val orig = ConnectionPoolSettings(system.settings.config).copy(idleTimeout = Duration.Inf)
    val clientSettings = orig.connectionSettings.withIdleTimeout(Duration.Inf)
    val settings = orig.copy(connectionSettings = clientSettings)

    val response: Future[HttpResponse] = if(request_entity.isDefined) {
      Http().singleRequest(request = HttpRequest(
        method = method,
        uri = uri,
        headers = httpHeader,
        entity = request_entity.get), settings = settings)
    } else {
      Http().singleRequest(request = HttpRequest(
        method = method,
        uri = uri,
        headers = httpHeader), settings = settings)
    }
    response
  }

  def forwardCreateItem(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {

    if(itemInfoService.itemInfoService.isEmpty) { //if empty refresh the map
      itemInfoService.updateItemInfoService(forward.index.get)
    }

    val itemInfoKey = forward.index.get + "." + forwardingDestination.item_info_id
    if (! itemInfoService.itemInfoService.contains(itemInfoKey)) {
      val message = "the item info is not defined for the key: " + itemInfoKey
      throw ForwardingException(message)
    }

    val itemInfoFilters = itemInfoService.itemInfoService.get(itemInfoKey)

    val uri = forwardingDestination.url + "/insertitems?unique_id=_id"
    val item = document.get

    /* base fields are: type, name, description */
    val baseFields: Map[String, Any] = itemInfoFilters.get.base_fields.toList.map({
      case "type" =>
        ("type", item.category)
      case "name" =>
        ("name", item.name)
      case "description" =>
        ("description", item.description)
      case _ =>
        (None.orNull, None.orNull)
    }).filter(_._1 != None.orNull).toMap

    val tags: CsrecItemType = if (item.props.isDefined) {
      val regex = itemInfoFilters.get.tag_filters
      val tagValues = item.props.get.tags.getOrElse(Array.empty[String]).filter(x => {
        x.matches(regex)
      })
      if(tagValues.nonEmpty) {
        Map("tags" -> tagValues)
      } else {
        Map.empty[String, Any]
      }
    } else {
      Map.empty[String, Any]
    }

    val stringProperties: CsrecItemType = if (item.props.isDefined) {
      val regex = itemInfoFilters.get.string_filters
      item.props.get.string.getOrElse(Array.empty[StringProperties]).filter(x => {
        x.key.matches(regex)
      }).map(x => {
        (x.key, x.value: Any)
      }).groupBy(_._1).map(x => {
        val array = x._2.map(y => {y._2.toString})
        (x._1, array)
      })
    } else {
      Map.empty[String, Any]
    }

    val csrecItem: CsrecItemsArray = Array(Map[String, Any](
      "_id" -> item.id
    ) ++ baseFields ++ stringProperties ++ tags)

    val entityFuture = Marshal(csrecItem).to[MessageEntity]
    val entity = Await.result(entityFuture, 1.second)
    val httpRequest = Await.result(
      executeHttpRequest(uri = uri, method = HttpMethods.POST, request_entity = Option{entity}), Duration.Inf)

    httpRequest.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forwardDeleteItem(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {
    val uri = forwardingDestination.url + "/item?item=" + forward.doc_id
    val httpRequest = Await.result(
      executeHttpRequest(uri = uri, method = HttpMethods.DELETE), Duration.Inf)
    httpRequest.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forwardItem(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {
    forward.operation match {
      case ForwardOperationType.create =>
        forwardCreateItem(forward = forward, document = document)
      case ForwardOperationType.delete =>
        forwardDeleteItem(forward = forward, document = document)
      case ForwardOperationType.update =>
        forwardDeleteItem(forward = forward, document = document)
        forwardCreateItem(forward = forward, document = document)
    }
  }

  def forwardDeleteOracUser(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit = {
    log.debug("called forwarding delete orac user for csrec: " + forward.doc_id)
    val uri = forwardingDestination.url + "/user?user_id=" +  forward.doc_id

    val httpRequest = Await.result(executeHttpRequest(uri = uri, method = HttpMethods.DELETE), Duration.Inf)
    httpRequest.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forwardOracUser(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit = {
    log.debug("called forwarding orac user for csrec: " + forward.doc_id)
    forward.operation match {
      case ForwardOperationType.delete =>
        forwardDeleteOracUser(forward = forward, document = document)
      case _ =>
        log.debug("called forwarding orac user for csrec: nothing to do")
    }
  }

  def forwardCreateAction(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {

    if(itemInfoService.itemInfoService.isEmpty) { //if empty refresh the map
      itemInfoService.updateItemInfoService(forward.index.get)
    }

    val itemInfoKey = forward.index.get + "." + forwardingDestination.item_info_id
    if (! itemInfoService.itemInfoService.contains(itemInfoKey)) {
      val message = "the item info is not defined for the key: " + itemInfoKey
      throw ForwardingException(message)
    }

    val itemInfoFilters = itemInfoService.itemInfoService.get(itemInfoKey)

    val doc = document.get
    val uri = forwardingDestination.url + "/itemaction?item=" +  doc.item_id + "&user=" + doc.user_id +
      "&code=" + doc.score.getOrElse(0.0) + "&only_info=false"

    val itemOption = Await.result(itemService.read(forward.index.get, List(doc.item_id)), Duration.Inf)
    if(itemOption.isEmpty || itemOption.get.items.isEmpty) {
      val message = "Cannot retrieve the item id(" + doc.item_id + ") required to forward the action"
      throw ForwardingException(message)
    }

    val item = itemOption.get.items.head

    /* base fields are: type, name, description */
    val baseFields: List[String] = itemInfoFilters.get.base_fields.toList.map({
      case "type" =>
        "type"
      case "name" =>
        "name"
      case "description" =>
        "description"
      case _ =>
        None.orNull
    }).filter(_ != None.orNull)

    val tags: List[String] = if(item.props.isDefined) {
      val regex = itemInfoFilters.get.tag_filters
      val tagValues = item.props.get.tags.getOrElse(Array.empty[String]).filter(x => {
        x.matches(regex)
      })
      if(tagValues.nonEmpty) {
        List("tags")
      } else {
        List.empty[String]
      }
    } else {
      List.empty[String]
    }

    val stringProperties: List[String] = if (item.props.isDefined) {
      val regex = itemInfoFilters.get.string_filters
      item.props.get.string.getOrElse(Array.empty[StringProperties]).filter(x => {
        x.key.matches(regex)
      }).map(x => {
        (x.key, x.value: Any)
      }).groupBy(_._1).map(x => (x._1, x._2.map(y => {y._2}))).keys.toList
    } else {
      List.empty[String]
    }

    val meaningfulFields = baseFields ++ tags ++ stringProperties
    val meaningfulItemInfo = Map[String, List[String]](
      "item_info" -> meaningfulFields,
    )

    val entityFuture = Marshal(meaningfulItemInfo).to[MessageEntity]
    val entity = Await.result(entityFuture, 1.second)

    val httpRequest = Await.result(
      executeHttpRequest(uri = uri, method = HttpMethods.POST, request_entity = Option{entity}), Duration.Inf)
    httpRequest.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forwardDeleteAction(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
    val uri = forwardingDestination.url + "/item?item=" + forward.doc_id
    val httpRequest = Await.result(executeHttpRequest(uri = uri, method = HttpMethods.DELETE), Duration.Inf)
    httpRequest.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") forward_type(" + forward.item_type + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forwardAction(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
    forward.operation match {
      case ForwardOperationType.create =>
        forwardCreateAction(forward = forward, document = document)
      case ForwardOperationType.delete =>
        forwardDeleteAction(forward = forward, document = document)
      case ForwardOperationType.update =>
        forwardDeleteAction(forward = forward, document = document)
        forwardCreateAction(forward = forward, document = document)
    }
  }

  def getRecommendations(user_id: String, from: Int = 0,
                         size: Int = 10): Option[Array[Recommendation]] = {
    val uri = forwardingDestination.url + "/recommend?user=" + user_id + "&limit=" + size
    val httpRequest = Await.result(executeHttpRequest(uri = uri, method = HttpMethods.GET), Duration.Inf)
    val recommendations = httpRequest.status match {
      case StatusCodes.OK =>
        val csrecRecommFuture = Unmarshal(httpRequest.entity).to[Map[String, Array[String]]]
        val csrecRecomm = Await.result(csrecRecommFuture, 2.second)
        val generationTimestamp = Time.timestampMillis
        val generationBatch = "csrec_" + RandomNumbers.intPos + "_" + generationTimestamp
        val recommendationArray = csrecRecomm.getOrElse("items", Array.empty[String]).map(x => {
          val recommendationId = Checksum.sha512(generationBatch + RandomNumbers.intPos
            + RandomNumbers.intPos + x)
          Recommendation(id = Option{recommendationId}, user_id = user_id, item_id = x,
            name = "csrec_recommendation",
            generation_batch = generationBatch, generation_timestamp = generationTimestamp, score = 0.0)
        })
        Option { recommendationArray }
      case _ =>
        val message = "No recommendation for the user from csrec: " + user_id
        log.info(message)
        Option.empty[Array[Recommendation]]
    }
    recommendations
  }
}
