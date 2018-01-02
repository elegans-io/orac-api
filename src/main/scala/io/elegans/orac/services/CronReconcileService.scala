package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import akka.actor.Actor
import io.elegans.orac.entities._
import akka.actor.Props

import scala.util.{Failure, Success, Try}
import scala.language.postfixOps

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
    val index_check = systemIndexManagementService.check_index_status
    if (index_check) {
      val iterator = reconcileService.getAllDocuments
      iterator.foreach(item  => {
        val item_type = item.`type`
        item_type match {
          case ReconcileType.orac_user =>
            val reconcile_res = Try(reconcileService.reconcileUser(item.index.get, item))
            if(item.retry > 0) {
              reconcile_res match {
                case Success(t) =>
                  log.info("Reconciliation: successfully completed: " + item.toString)
                  val history_element = ReconcileHistory(old_id = item.old_id, new_id = item.new_id,
                    index = item.index, `type` = item.`type`, index_suffix = item.index_suffix,
                    insert_timestamp = item.timestamp.get, retry = item.retry)
                  val reconcile_history_insert =
                    Await.result(reconcileHistoryService.create(history_element, 0), 5.seconds)
                  if(reconcile_history_insert.isDefined) {
                    val reconcile_item_delete =
                      Await.result(reconcileService.delete(item.id.get, item.index.get, 0), 5.seconds)
                    if(reconcile_item_delete.isEmpty) {
                      log.error("Reconciliation: can't delete the entry(" + item.id.get
                        + ") from the reconciliation table")
                    }
                  } else {
                    log.error("Reconciliation: can't create the reconciliation item entry: " + item.id.get)
                  }
                case Failure(e) =>
                  log.error("Reconciliation: failed : " + item.toString())
                  val updateReconcile = UpdateReconcile(retry = Option {
                    item.retry - 1
                  })
                  reconcileService.update(id = item.id.get, document = updateReconcile, 0)
              }
            } else {
              log.info("Reconciliation: removing entry: " + item.id)
              reconcileService.delete(item.id.get, item.index.get, 0)
            }
        }
      })
    }
  }

  def reloadEventsOnce(): Unit = {
    val reloadDecisionTableActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.scheduleOnce(0 seconds, reloadDecisionTableActorRef, Tick)
  }

  def reloadEvents(): Unit = {
    val reloadDecisionTableActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.schedule(
      0 seconds,
      60 seconds,
      reloadDecisionTableActorRef,
      Tick)
  }

}
