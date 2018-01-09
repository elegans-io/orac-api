package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._

import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import io.elegans.orac.serializers.JsonSupport
import akka.http.scaladsl.model._
import spray.json.SerializationException
import spray.json._
import akka.http.scaladsl.marshalling.{Marshal, Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.{ContentType, HttpEntity, MediaTypes}
import akka.http.scaladsl.unmarshalling.Unmarshal
import io.elegans.orac.tools._

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
    val response: Future[HttpResponse] = if(request_entity.isDefined) {
      Http().singleRequest(HttpRequest(
        method = method,
        uri = uri,
        headers = httpHeader,
        entity = request_entity.get))
    } else {
      Http().singleRequest(HttpRequest(
        method = method,
        uri = uri,
        headers = httpHeader))
    }
    response
  }

  def forward_create_item(forward: Forward, document: Option[Item] = Option.empty[Item],
                          item_info_filters: Option[ItemInfo]): Unit = {
    val uri = forwardingDestination.url + "/insertitems?unique_id=_id"
    val item = document.get

    /* base fields are: type, name, description */
    val base_fields: Map[String, Any] = item_info_filters.get.base_fields.toList.map({
      case "type" =>
        ("type", item.`type`)
      case "name" =>
        ("name", item.name)
      case "description" =>
        ("description", item.description)
      case _ =>
        (null, null)
    }).filter(_._1 != null).toMap

    val tags: CsrecItemType = if (item.properties.isDefined) {
      val regex = item_info_filters.get.tag_filters
      val tag_values = item.properties.get.tags.getOrElse(Array.empty[String]).filter(x => {
        x.matches(regex)
      })
      if(tag_values.nonEmpty) {
        Map("tags" -> tag_values)
      } else {
        Map.empty[String, Any]
      }
    } else {
      Map.empty[String, Any]
    }

    val string_properties: CsrecItemType = if (item.properties.isDefined) {
      val regex = item_info_filters.get.string_filters
      item.properties.get.string.getOrElse(Array.empty[StringProperties]).filter(x => {
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

    val csrec_item: CsrecItemsArray = Array(Map[String, Any](
      "_id" -> item.id,
    ) ++ base_fields ++ string_properties ++ tags)

    val entity_future = Marshal(csrec_item).to[MessageEntity]
    val entity = Await.result(entity_future, 1.second)
    val http_request = Await.result(
      executeHttpRequest(uri = uri, method = HttpMethods.POST, request_entity = Option{entity}), 5.seconds)

    http_request.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forward_delete_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {
    val uri = forwardingDestination.url + "/item?item=" + forward.doc_id
    val http_request = Await.result(
      executeHttpRequest(uri = uri, method = HttpMethods.DELETE), 5.seconds)
    http_request.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forward_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {
    if(itemInfoService.item_info_service.isEmpty) { //if empty refresh the map
      itemInfoService.updateItemInfoService(forward.index)
    }

    val item_info_key = forward.index + "." + forwardingDestination.item_info_id
    if (! itemInfoService.item_info_service.contains(item_info_key)) {
      val message = "the item info is not defined for the key: " + item_info_key
      throw ForwardingException(message)
    }

    val item_info_filters = itemInfoService.item_info_service.get(item_info_key)

    forward.operation match {
      case "create" =>
        forward_create_item(forward = forward, document = document, item_info_filters = item_info_filters)
      case "delete" =>
        forward_delete_item(forward = forward, document = document)
      case "update" =>
        forward_delete_item(forward = forward, document = document)
        forward_create_item(forward = forward, document = document, item_info_filters = item_info_filters)
    }
  }

  def forward_delete_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit = {
    log.debug("called forwarding delete orac user for csrec: " + forward.doc_id)
    val uri = forwardingDestination.url + "/user?user_id=" +  forward.doc_id

    val http_request = Await.result(executeHttpRequest(uri = uri, method = HttpMethods.DELETE), 5.seconds)
    http_request.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forward_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit = {
    log.debug("called forwarding orac user for csrec: " + forward.doc_id)
    forward.operation match {
      case "delete" =>
        forward_delete_orac_user(forward = forward, document = document)
      case _ =>
        log.debug("called forwarding orac user for csrec: nothing to do")
    }
  }

  def forward_create_action(forward: Forward, document: Option[Action] = Option.empty[Action],
                            item_info_filters: Option[ItemInfo]): Unit = {
    val doc = document.get
    val uri = forwardingDestination.url + "/itemaction?item=" +  doc.item_id + "&user=" + doc.user_id +
      "&code=" + doc.score.getOrElse(0.0) + "&only_info=false"

    val item_option = Await.result(itemService.read(forward.index, List(doc.item_id)), 5.second)
    if(item_option.isEmpty || item_option.get.items.isEmpty) {
      val message = "Cannot retrieve the item id(" + doc.item_id + ") required to forward the action"
      throw ForwardingException(message)
    }

    val item = item_option.get.items.head

    /* base fields are: type, name, description */
    val base_fields: List[String] = item_info_filters.get.base_fields.toList.map({
      case "type" =>
        "type"
      case "name" =>
        "name"
      case "description" =>
        "description"
      case _ =>
        null
    }).filter(_ != null)

    val tags: List[String] = if(item.properties.isDefined) {
      val regex = item_info_filters.get.tag_filters
      val tag_values = item.properties.get.tags.getOrElse(Array.empty[String]).filter(x => {
        x.matches(regex)
      })
      if(tag_values.nonEmpty) {
        List("tags")
      } else {
        List.empty[String]
      }
    } else {
      List.empty[String]
    }

    val string_properties: List[String] = if (item.properties.isDefined) {
      val regex = item_info_filters.get.string_filters
      item.properties.get.string.getOrElse(Array.empty[StringProperties]).filter(x => {
        x.key.matches(regex)
      }).map(x => {
        (x.key, x.value: Any)
      }).groupBy(_._1).map(x => (x._1, x._2.map(y => {y._2}))).keys.toList
    } else {
      List.empty[String]
    }

    val meaningful_fields = base_fields ++ tags ++ string_properties
    val meaningful_item_info = Map[String, List[String]](
      "item_info" -> meaningful_fields,
    )

    val entity_future = Marshal(meaningful_item_info).to[MessageEntity]
    val entity = Await.result(entity_future, 1.second)

    val http_request = Await.result(
      executeHttpRequest(uri = uri, method = HttpMethods.POST, request_entity = Option{entity}), 5.seconds)
    http_request.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forward_delete_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
    val uri = forwardingDestination.url + "/item?item=" + forward.doc_id
    val http_request = Await.result(executeHttpRequest(uri = uri, method = HttpMethods.DELETE), 5.seconds)
    http_request.status match {
      case StatusCodes.Created | StatusCodes.OK =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        log.debug(message)
      case _ =>
        val message = "index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + uri + ")"
        throw ForwardingException(message)
    }
  }

  def forward_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
    if(itemInfoService.item_info_service.isEmpty) { //if empty refresh the map
      itemInfoService.updateItemInfoService(forward.index)
    }

    val item_info_key = forward.index + "." + forwardingDestination.item_info_id
    if (! itemInfoService.item_info_service.contains(item_info_key)) {
      val message = "the item info is not defined for the key: " + item_info_key
      throw ForwardingException(message)
    }

    val item_info_filters = itemInfoService.item_info_service.get(item_info_key)

    forward.operation match {
      case "create" =>
        forward_create_action(forward = forward, document = document, item_info_filters = item_info_filters)
      case "delete" =>
        forward_delete_action(forward = forward, document = document)
      case "update" =>
        forward_delete_action(forward = forward, document = document)
        forward_create_action(forward = forward, document = document, item_info_filters = item_info_filters)
    }
  }

  def get_recommendations(user_id: String, from: Int = 0,
                          size: Int = 10): Option[Array[Recommendation]] = {
    val uri = forwardingDestination.url + "/recommend?user=" + user_id + "&limit=" + size
    val http_request = Await.result(executeHttpRequest(uri = uri, method = HttpMethods.GET), 10.seconds)
    val recommendations = http_request.status match {
      case StatusCodes.OK =>
        val csrec_recomm_future = Unmarshal(http_request.entity).to[Map[String, Array[String]]]
        val csrec_recomm = Await.result(csrec_recomm_future, 2.second)
        val generation_timestamp = Time.getTimestampMillis
        val generation_batch = "csrec_" + RandomNumbers.getIntPos + "_" + generation_timestamp
        val recommendation_array = csrec_recomm.getOrElse("items", Array.empty[String]).map(x => {
          val recommendation_id = Checksum.sha512(generation_batch + RandomNumbers.getIntPos
            + RandomNumbers.getIntPos + x)
          Recommendation(id = Option{recommendation_id}, user_id = user_id, item_id = x,
            name = "csrec_recommendation",
            generation_batch = generation_batch, generation_timestamp = generation_timestamp, score = 0.0)
        })
        Option { recommendation_array }
      case _ =>
        val message = "No recommendation for the user from csrec: " + user_id
        log.info(message)
        Option.empty[Array[Recommendation]]
    }
    recommendations
  }
}
