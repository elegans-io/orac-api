package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import akka.actor.{Actor, ActorRef, Props}
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities.{Action, ForwardType, Item, OracUser}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

object CronForwardEventsService {
  implicit def executionContext: ExecutionContext = OracActorSystem.system.dispatcher
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService
  val systemIndexManagementService: SystemIndexManagementService.type = SystemIndexManagementService
  val forwardService: ForwardService.type = ForwardService

  val tickCheckForwardingItems = "tick"

  class ForwardEventsTickActor extends Actor {
    def receive: PartialFunction[Any, Unit] = {
      case `tickCheckForwardingItems` =>
        forwardingProcess()
      case _ =>
        log.error("Unknown error in forwarding process")
    }
  }

  def forwardingProcess(): Unit = {
    val indexCheck = systemIndexManagementService.checkIndexStatus
    if (indexCheck) {
      val iterator = forwardService.allDocuments()
      iterator.foreach(fwdItem => {
        var deleteItem = false
        log.debug("forwarding item: " + fwdItem)
        val index = fwdItem.index.get
        forwardService.forwardingDestinations.getOrElse(index, List.empty).foreach(item => {
          val forwarder = item._2
          fwdItem.item_type match {
            case ForwardType.item =>
              val ids = List(fwdItem.doc_id)
              val result = Await.result(itemService.read(index, ids), 5.seconds)
              result match {
                case Some(document) =>
                  val forwardDoc = if (document.items.nonEmpty) {
                    Option {
                      document.items.head
                    }
                  } else {
                    Option.empty[Item]
                  }

                  val tryResponse = Try(forwarder.forwardItem(fwdItem, forwardDoc))
                  tryResponse match {
                    case Success(t) =>
                      deleteItem = true
                    case Failure(e) =>
                      log.error("forward item: " + e.getMessage)
                  }
                case _ =>
                  log.error("Error retrieving document: " + fwdItem.doc_id + " from " + fwdItem.index + ":" +
                    fwdItem.item_type)
              }
            case ForwardType.action =>
              val ids = List(fwdItem.doc_id)
              val result = Await.result(actionService.read(index, ids), 5.seconds)
              result match {
                case Some(document) =>
                  val forwardDoc = if (document.items.nonEmpty) {
                    Option {
                      document.items.head
                    }
                  } else {
                    Option.empty[Action]
                  }

                  val tryResponse = Try(forwarder.forwardAction(fwdItem, forwardDoc))
                  tryResponse match {
                    case Success(t) =>
                      deleteItem = true
                    case Failure(e) =>
                      log.error("forward action: " + e.getMessage)
                  }
                case _ =>
                  log.error("Error retrieving document: " + fwdItem.doc_id + " from " + fwdItem.index + ":" +
                    fwdItem.item_type)
              }
            case ForwardType.orac_user =>
              val ids = List(fwdItem.doc_id)
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

                  val tryResponse = Try(forwarder.forwardOracUser(fwdItem, forward_doc))
                  tryResponse match {
                    case Success(t) =>
                      deleteItem = true
                    case Failure(e) =>
                      log.error("forward orac user: " + e.getMessage)
                  }
                case _ =>
                  log.error("Error retrieving document: " + fwdItem.doc_id + " from " + fwdItem.index + ":" +
                    fwdItem.item_type)
              }
          }
        })

        // deleting item from forwarding table
        if (deleteItem) {
          val result = Await.result(
            forwardService.delete(indexName = index, id = fwdItem.id.get, refresh = 0), 5.seconds)
        }
      })

    } else {
      log.warning("System index is still not initialized or broken")
    }
  }

  def sendEvent(): Unit = {
    val updateEventsActorRef: ActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.scheduleOnce(0 seconds, updateEventsActorRef, tickCheckForwardingItems)
  }

  def reloadEvents(): Unit = {
    val updateEventsActorRef: ActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.schedule(
      0 seconds,
      1 seconds,
      updateEventsActorRef,
      tickCheckForwardingItems)
  }

}
