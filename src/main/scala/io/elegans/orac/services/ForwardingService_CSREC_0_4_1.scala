package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities.{Action, Forward, Item, OracUser}
import io.elegans.orac.serializers._
import scala.util.{Failure, Success, Try}
import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class ForwardingService_CSREC_0_4_1(forwardingDestination: ForwardingDestination)
  extends AbstractForwardingImplService with JsonSupport {
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val httpHeader: immutable.Seq[HttpHeader] = immutable.Seq(RawHeader("application", "json"))

  implicit val system = OracActorSystem.system
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

  def handleRequestResult(response: Future[HttpResponse], forward: Forward): Unit = {
    val try_response = Try(Await.ready(response, 5.seconds))
    try_response match {
      case Success(t) =>
        t.onComplete( _ match {
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
  
  def forward_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit = {
    forward.operation match {
      case "create" | "update" =>
        val doc = document.get

        // example csrec data structure
        // { "_id" : "item1", "type": "lady", "category" : "high_heels"}
        // { "_id" : "item2", "type": "male", "category" : "mocassino"}

        val entity_future = Marshal(doc).to[MessageEntity]
        val entity = Await.result(entity_future, 1.second)
        val responseFuture: Future[HttpResponse] =
          Http(). singleRequest(HttpRequest(
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

  def forward_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit = {
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

  def forward_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit = {
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
