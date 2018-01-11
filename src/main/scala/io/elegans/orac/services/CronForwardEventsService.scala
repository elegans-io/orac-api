package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import akka.actor.Actor
import io.elegans.orac.entities.{Action, Item, OracUser, ForwardType}
import akka.actor.Props
import scala.util.{Failure, Success, Try}
import akka.actor.ActorRef
import scala.language.postfixOps

object CronForwardEventsService {
  implicit def executionContext: ExecutionContext = OracActorSystem.system.dispatcher
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService
  val systemIndexManagementService: SystemIndexManagementService.type = SystemIndexManagementService
  val forwardService: ForwardService.type = ForwardService

  val Tick = "tick"

  class ForwardEventsTickActor extends Actor {
    def receive: PartialFunction[Any, Unit] = {
      case Tick =>
        forwardingProcess()
      case _ =>
        log.error("Unknown error in forwarding process")
    }
  }

  def forwardingProcess(): Unit = {
    val index_check = systemIndexManagementService.check_index_status
    if (index_check) {
      var delete_item = false
      val iterator = forwardService.getAllDocuments
      iterator.foreach(fwd_item => {
        val index = fwd_item.index.get
        forwardService.forwardingDestinations.getOrElse(index, List.empty).foreach(item => {
          val forwarder = item._2
          fwd_item.`type` match {
            case ForwardType.item =>
              val ids = List(fwd_item.doc_id)
              val result = Await.result(itemService.read(index, ids), 5.seconds)
              result match {
                case Some(document) =>
                  val forward_doc = if (document.items.nonEmpty) {
                    Option {
                      document.items.head
                    }
                  } else {
                    Option.empty[Item]
                  }

                  val try_response = Try(forwarder.forward_item(fwd_item, forward_doc))
                  try_response match {
                    case Success(t) =>
                      delete_item = true
                    case Failure(e) =>
                      log.error("forward item: " + e.getMessage)
                  }
                case _ =>
                  log.error("Error retrieving document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                    fwd_item.`type`)
              }
            case ForwardType.action =>
              val ids = List(fwd_item.doc_id)
              val result = Await.result(actionService.read(index, ids), 5.seconds)
              result match {
                case Some(document) =>
                  val forward_doc = if (document.items.nonEmpty) {
                    Option {
                      document.items.head
                    }
                  } else {
                    Option.empty[Action]
                  }

                  val try_response = Try(forwarder.forward_action(fwd_item, forward_doc))
                  try_response match {
                    case Success(t) =>
                      delete_item = true
                    case Failure(e) =>
                      log.error("forward action: " + e.getMessage)
                  }
                case _ =>
                  log.error("Error retrieving document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                    fwd_item.`type`)
              }
            case ForwardType.orac_user =>
              val ids = List(fwd_item.doc_id)
              val result = Await.result(oracUserService.read(index, ids), 5.seconds)
              result match {
                case Some(document) =>
                  val forward_doc = if (document.items.nonEmpty) {
                    Option {
                      document.items.head
                    }
                  } else {
                    Option.empty[OracUser]
                  }

                  val try_response = Try(forwarder.forward_orac_user(fwd_item, forward_doc))
                  try_response match {
                    case Success(t) =>
                      delete_item = true
                    case Failure(e) =>
                      log.error("forward orac user: " + e.getMessage)
                  }
                case _ =>
                  log.error("Error retrieving document: " + fwd_item.doc_id + " from " + fwd_item.index + ":" +
                    fwd_item.`type`)
              }
          }
        })

        // deleting item from forwarding table
        if (delete_item) {
          val result = Await.result(
            forwardService.delete(index_name = index, id = fwd_item.id.get, refresh = 0), 5.seconds)
          delete_item = false
        }
      })

    } else {
      log.warning("System index is still not initialized or broken")
    }
  }

  def sendEvent(): Unit = {
    val updateEventsActorRef: ActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.scheduleOnce(0 seconds, updateEventsActorRef, Tick)
  }

  def reloadEvents(): Unit = {
    val updateEventsActorRef: ActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.schedule(
      0 seconds,
      1 seconds,
      updateEventsActorRef,
      Tick)
  }

}
