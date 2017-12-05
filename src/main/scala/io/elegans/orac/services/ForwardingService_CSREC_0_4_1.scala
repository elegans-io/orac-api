package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.{Marshal, ToEntityMarshaller}
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._

import scala.util.{Failure, Success, Try}
import scala.collection.immutable
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration._
import akka.actor.ActorSystem
import io.elegans.orac.serializers.JsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.marshalling.Marshaller
import spray.json.SerializationException
import spray.json._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.marshalling.{Marshaller, ToEntityMarshaller}
import akka.http.scaladsl.model.{ContentType, HttpEntity, MediaTypes}

class ForwardingService_CSREC_0_4_1(forwardingDestination: ForwardingDestination)
  extends AbstractForwardingImplService with JsonSupport {
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val httpHeader: immutable.Seq[HttpHeader] = immutable.Seq(RawHeader("application", "json"))

  implicit val system: ActorSystem = OracActorSystem.system
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def handleRequestResult(response: Future[HttpResponse], forward: Forward): Unit = {
    val try_response = Try(Await.ready(response, 5.seconds))
    try_response match {
      case Success(t) =>
        t.onComplete({
          case Success(k) =>
            k.status match {
              case StatusCodes.Created | StatusCodes.OK =>
                log.debug("index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
                  " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
                  " destination(" + forwardingDestination.url + ")")
              case _ =>
                log.error("index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
                  " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
                  " destination(" + forwardingDestination.url + ")")
            }
          case Failure(e) =>
            log.error("Failed to forward event: " + e.getMessage)
        })
      case Failure(e) =>
        log.error("Failed to forward event: " + e.getMessage)
    }
  }

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

  def forward_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {
    forward.operation match {
      case "create" | "update" =>
        val uri = forwardingDestination.url + "/insertitems?unique_id=_id"
        val doc = document.get

        val tags: CsrecItemType = if(doc.properties.isDefined){
          val tagvalues = doc.properties.get.tags.getOrElse(Array.empty[String])
          Map("tags" -> tagvalues)
        } else {
          Map.empty[String, Any]
        }

        val string_properties: CsrecItemType = if (doc.properties.isDefined) {
          doc.properties.get.string.getOrElse(Array.empty[StringProperties]).map(x => {
            (x.key, x.value: Any)
          }).toMap
        } else {
          Map.empty[String, Any]
        }

        val numerical_properties: CsrecItemType = if (doc.properties.isDefined) {
          doc.properties.get.numerical.getOrElse(Array.empty[NumericalProperties]).map(x => {
            (x.key, x.value: Any)
          }).toMap
        } else {
          Map.empty[String, Any]
        }

        val csrec_item: CsrecItemsArray = Array(Map[String, Any](
          "_id" -> doc.id,
          "type" -> doc.`type`,
        ) ++ string_properties ++ tags ++ numerical_properties)

        val entity_future = Marshal(csrec_item).to[MessageEntity]
        val entity = Await.result(entity_future, 1.second)
        val responseFuture: Future[HttpResponse] =
          Http(). singleRequest(HttpRequest(
            method = HttpMethods.POST,
            uri = uri,
            headers = httpHeader,
            entity = entity))
        handleRequestResult(responseFuture, forward)
      case "delete" =>
        val uri = forwardingDestination.url + "/item?item=" + forward.doc_id
        val responseFuture: Future[HttpResponse] =
          Http(). singleRequest(HttpRequest(
            method = HttpMethods.DELETE,
            uri = uri,
            headers = httpHeader))
        handleRequestResult(responseFuture, forward)
    }
  }

  def forward_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit = {
    log.debug("called forwarding user for csrec")
  }

  def forward_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
    //curl -X POST  -H "Content-Type: application/json" -d '{ "item_info" : ["type", "category"]}' 'http://elegans.it:8000/itemaction?item=item1&user=User1&code=1&only_info=false'
    forward.operation match {
      case "create" | "update" =>
        val doc = document.get
        val uri = forwardingDestination.url + "/itemaction?item=" +  doc.item_id + "&user=" + doc.user_id +
          "&code=" + doc.score + "&only_info=false"

        val csrec_item: CsrecItemsArray = Array(Map[String, Any](
          "_id" -> doc.id
        ))

        val entity_future = Marshal(csrec_item).to[MessageEntity]
        val entity = Await.result(entity_future, 1.second)
        val responseFuture: Future[HttpResponse] =
          Http(). singleRequest(HttpRequest(
            method = HttpMethods.POST,
            uri = uri,
            headers = httpHeader,
            entity = entity))
        handleRequestResult(responseFuture, forward)
      case "delete" =>
        val doc = document.get
        val uri = forwardingDestination.url + "/itemaction?item_id=" +  doc.item_id + "&user_id=" + doc.user_id
        val responseFuture: Future[HttpResponse] =
          Http(). singleRequest(HttpRequest(
            method = HttpMethods.DELETE,
            uri = uri,
            headers = httpHeader))
        handleRequestResult(responseFuture, forward)

    }
  }

  def get_recommendations(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
    forward.operation match {
      case "create" | "update" =>
        val doc = document.get
        val entity_future = Marshal(doc).to[MessageEntity]
        val entity = Await.result(entity_future, 1.second)
        val responseFuture: Future[HttpResponse] =
          Http().singleRequest(HttpRequest(
            method = HttpMethods.POST,
            uri = forwardingDestination.url,
            headers = httpHeader,
            entity = entity))
        handleRequestResult(responseFuture, forward)
      case "delete" =>
        log.error("Deleting -> index(" + forward.index + ") index_suffix(" + forward.index_suffix + ")" +
          " operation(" + forward.operation + ") docid(" + forward.doc_id + ")" +
          " destination(" + forwardingDestination.url + ")")
    }
  }
}
