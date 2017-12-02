package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import akka.actor.Actor
import akka.actor.Props
import io.elegans.orac.services.ForwardService.{getAllDocuments, log}

import scala.language.postfixOps

class CronForwardEventsService (implicit val executionContext: ExecutionContext) {
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val itemService = ItemService
  val oracUserService = OracUserService
  val actionService = ActionService
  val systemIndexManagementService = SystemIndexManagementService
  val forwardService = ForwardService

  val Tick = "tick"

  class ForwardEventsTickActor extends Actor {
    def receive = {
      case Tick =>
        forwardingProcess()
      case _ =>
        log.error("Unknown error in forwarding process")
    }
  }

  def forwardingProcess(): Unit = {
    val index_check = systemIndexManagementService.check_index_status
    if (index_check) {
      val iterator = forwardService.getAllDocuments()
      iterator.foreach(fwd_item  => {
        forwardService.forwardingDestinations.foreach(item => {
          val forwarder = item._2
          val index = fwd_item.index
          fwd_item.index_suffix match {
            case itemService.elastic_client.item_index_suffix =>
              fwd_item.operation match {
                case "create" | "update" =>
                  val ids = List (fwd_item.doc_id)
                  val result = Await.result (itemService.read (index, ids), 5.seconds)
                  result match {
                    case Some (document) =>
                      if (document.items.nonEmpty) {
                        forwarder.forward_item (fwd_item, Option{document.items.head})
                      } else {
                        log.error ("Cannot find the document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                          fwd_item.index_suffix)
                      }
                    case _ =>
                      log.error ("Error retrieving document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                        fwd_item.index_suffix)
                  }
                case "delete" =>
                  forwarder.forward_item(fwd_item)
              }
            case actionService.elastic_client.action_index_suffix =>
              fwd_item.operation match {
                case "create" | "update" =>
                  val ids = List(fwd_item.doc_id)
                  val result = Await.result(actionService.read(index, ids), 5.seconds)
                  result match {
                    case Some(document) =>
                      if (document.items.nonEmpty) {
                        forwarder.forward_action(fwd_item, Option {
                          document.items.head
                        })
                      } else {
                        log.error("Cannot find the document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                          fwd_item.index_suffix)
                      }
                    case _ =>
                      log.error("Error retrieving document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                        fwd_item.index_suffix)
                  }
                case "delete" =>
                  forwarder.forward_action(fwd_item)
              }
            case oracUserService.elastic_client.orac_user_index_suffix =>
              fwd_item.operation match {
                case "create" | "update" =>
                  val ids = List(fwd_item.doc_id)
                  println("AAAA: ", ids, index)
                  val result = Await.result(oracUserService.read(index, ids), 5.seconds)
                  result match {
                    case Some(document) =>
                      if (document.items.nonEmpty) {
                        forwarder.forward_orac_user(fwd_item, Option{document.items.head})
                      } else {
                        log.error("Cannot find the document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                          fwd_item.index_suffix)                  }
                    case _ =>
                      log.error("Error retrieving document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                        fwd_item.index_suffix)
                  }
                case "delete" =>
                  forwarder.forward_orac_user(fwd_item)
              }
          }
        })

        // deleting item from forwarding table
        forwardService.delete(id = fwd_item.id.get, refresh = 0)
      })

    } else {
      log.warning("System index is still not initialized or broken")
    }
  }

  def reloadAnalyzers(): Unit = {
    val reloadDecisionTableActorRef = OracActorSystem.system.actorOf(Props(classOf[ForwardEventsTickActor], this))
    OracActorSystem.system.scheduler.schedule(
      0 seconds,
      1 seconds,
      reloadDecisionTableActorRef,
      Tick)
  }

}
