package io.elegans.orac

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.resources._
import io.elegans.orac.services.{CronForwardEventsService, _}

import scala.concurrent.ExecutionContext

trait Resources extends RootAPIResource with SystemIndexManagementResource with IndexManagementResource
  with UserResource with ActionResource with ItemResource with RecommendationResource
  with RecommendationHistoryResource
  with OracUserResource
  with ForwardResource
  with ItemInfoResource
  with ReconcileResource
  with ReconcileHistoryResource

trait RestInterface extends Resources {
  implicit def executionContext: ExecutionContext

  lazy val initActionService: ActionService.type = ActionService
  lazy val initOracUserService: OracUserService.type = OracUserService
  lazy val initIndexManagementService: IndexManagementService.type = IndexManagementService
  lazy val initSystemIndexManagementService: SystemIndexManagementService.type = SystemIndexManagementService
  lazy val initItemService: ItemService.type = ItemService
  lazy val initInfoItemService: ItemInfoService.type = ItemInfoService
  lazy val initRecommendationService: RecommendationService.type = RecommendationService
  lazy val initUserService: UserService.type = UserService
  lazy val initForwardService: ForwardService.type = ForwardService
  lazy val initCronForwardEventsService: CronForwardEventsService.type = CronForwardEventsService
  lazy val initReconcileService: ReconcileService.type = ReconcileService
  lazy val initCronReconcileService: CronReconcileService.type = CronReconcileService
  lazy val initReconcileHistoryService: ReconcileHistoryService.type = ReconcileHistoryService

  val routes: Route = LoggingEntities.logRequestAndResult(systemGetIndexesRoutes) ~
    LoggingEntities.logRequestAndResultB64(systemIndexManagementRoutes) ~
    LoggingEntities.logRequestAndResultB64(postSystemIndexManagementOpenCloseRoutes) ~
    LoggingEntities.logRequestAndResultB64(postIndexManagementCreateRoutes) ~
    LoggingEntities.logRequestAndResultB64(postIndexManagementRefreshRoutes) ~
    LoggingEntities.logRequestAndResultB64(putIndexManagementRoutes) ~
    LoggingEntities.logRequestAndResultB64(indexManagementRoutes) ~
    LoggingEntities.logRequestAndResultB64(postIndexManagementOpenCloseRoutes) ~
    LoggingEntities.logRequestAndResult(postUserRoutes) ~
    LoggingEntities.logRequestAndResult(putUserRoutes) ~
    LoggingEntities.logRequestAndResult(deleteUserRoutes) ~
    LoggingEntities.logRequestAndResult(getUserRoutes) ~
    LoggingEntities.logRequestAndResult(genUserRoutes) ~
    LoggingEntities.logRequestAndResultReduced(actionStreamRoutes) ~
    LoggingEntities.logRequestAndResultReduced(actionRoutes) ~
    LoggingEntities.logRequestAndResultReduced(actionUserRoutes) ~
    LoggingEntities.logRequestAndResultReduced(itemStreamRoutes) ~
    LoggingEntities.logRequestAndResult(itemRoutes) ~
    LoggingEntities.logRequestAndResult(itemInfoRoutes) ~
    LoggingEntities.logRequestAndResult(oracUserRoutes) ~
    LoggingEntities.logRequestAndResultReduced(oracUserStreamRoutes) ~
    LoggingEntities.logRequestAndResult(queryRecommendationRoutes) ~
    LoggingEntities.logRequestAndResult(recommendationRoutes) ~
    LoggingEntities.logRequestAndResult(recommendationHistoryRoutes) ~
    LoggingEntities.logRequestAndResult(userRecommendationRoutes) ~
    LoggingEntities.logRequestAndResult(forwardRoutes) ~
    LoggingEntities.logRequestAndResult(forwardAllRoutes) ~
    LoggingEntities.logRequestAndResult(reconcileRoutes) ~
    LoggingEntities.logRequestAndResult(reconcileHistoryRoutes) ~
    LoggingEntities.logRequestAndResult(reconcileAllRoutes) ~
    LoggingEntities.logRequestAndResult(rootAPIsRoutes)

}
