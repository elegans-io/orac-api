package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

import akka.actor.{Actor, Props}
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps
import scala.util.{Failure, Success}

object CronReconcileService  {
  implicit def executionContext: ExecutionContext = OracActorSystem.system.dispatcher
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val itemService: ItemService.type = ItemService
  val oracUserService: OracUserService.type = OracUserService
  val actionService: ActionService.type = ActionService
  val systemIndexManagementService: SystemIndexManagementService.type = SystemIndexManagementService
  val reconcileService: ReconcileService.type = ReconcileService
  val reconcileHistoryService: ReconcileHistoryService.type = ReconcileHistoryService

  val Tick = "tick"

  class ForwardEventsTickActor extends Actor {
    def receive: PartialFunction[Any, Unit] = {
      case Tick =>
        reconcileProcess()
      case _ =>
        log.error("Unknown error in reconciliation process")
    }
  }

  def reconcileProcess(): Unit = {
    val indexCheck = systemIndexManagementService.checkIndexStatus
    if (indexCheck) {
      val iterator = reconcileService.getAllDocuments(60000)
      iterator.foreach(item  => {
        val itemType = item.`type`
        itemType match {
          case ReconcileType.orac_user =>
            if(item.retry > 0) {
              val reconcile_res = reconcileService.reconcileUser(item.index.get, item)
              reconcile_res.onComplete {
                case Success(t) =>
                  log.info("Reconciliation: successfully completed: " + item.toString)
                  val history_element = ReconcileHistory(old_id = item.old_id, new_id = item.new_id,
                    index = item.index, `type` = item.`type`,
                    insert_timestamp = item.timestamp.get, retry = item.retry)
                  val reconcileHistoryInsert =
                    Await.result(reconcileHistoryService.create(history_element, 0), 5.seconds)
                  if(reconcileHistoryInsert.isDefined) {
                    val reconcileItemDelete =
                      Await.result(reconcileService.delete(item.id.get, item.index.get, 0), 5.seconds)
                    if(reconcileItemDelete.isEmpty) {
                      log.error("Reconciliation: can't delete the entry(" + item.id.get
                        + ") from the reconciliation table")
                    }
                  } else {
                    log.error("Reconciliation: can't create the reconciliation item entry: " + item.id.get)
                  }
                case Failure(e) =>
                  log.error("Reconciliation: failed : " + item.toString + " : " + e.getMessage)
                  val updateReconcile = UpdateReconcile(retry = Option {
                    item.retry - 1
                  })
                  reconcileService.update(id = item.id.get, document = updateReconcile, 0)
              }
            } else {
              log.info("Reconciliation: max attemts reached (" + item.retry + ") ; removing entry: " + item.id)
              reconcileService.delete(item.id.get, item.index.get, 0)
            }
        }
      })
    }
  }

  def reloadEventsOnce(): Unit = {
    val updateEventsActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.scheduleOnce(0 seconds, updateEventsActorRef, Tick)
  }

  def reloadEvents(): Unit = {
    val updateEventsActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.schedule(
      0 seconds,
      1 seconds,
      updateEventsActorRef,
      Tick)
  }

}
