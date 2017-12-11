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

class CronReconcileService (implicit val executionContext: ExecutionContext) {
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
        item.`type` match {
          case ReconcileType.orac_user =>
            val reconcile_res = Try(reconcileService.reconcileUser(item.index, item))
            if(item.retry < 5) {
              reconcile_res match {
                case Success(t) =>
                  log.info("Reconciliation: successfully completed: " + item.toString)
                  val history_element = ReconcileHistory(old_id = item.old_id, new_id = item.new_id,
                    index = item.index, `type` = item.`type`, index_suffix = item.index_suffix,
                    insert_timestamp = item.timestamp.get, retry = item.retry)
                  reconcileHistoryService.create(history_element, 0)
                  reconcileService.delete(item.id.get, 0)
                case Failure(e) =>
                  log.error("Reconciliation: failed : " + item.toString())
                  val updateReconcile = UpdateReconcile(retry = Option {
                    item.retry + 1
                  })
                  reconcileService.update(id = item.id.get, document = updateReconcile, 0)
              }
            } else {
              log.info("Reconciliation: removing entry: " + item.id)
              reconcileService.delete(item.id.get, 0)
            }
        }
      })
    }
  }

  def reloadEvents(): Unit = {
    val reloadDecisionTableActorRef = OracActorSystem.system.actorOf(Props(new ForwardEventsTickActor))
    OracActorSystem.system.scheduler.schedule(
      0 seconds,
      1 seconds,
      reloadDecisionTableActorRef,
      Tick)
  }

}
