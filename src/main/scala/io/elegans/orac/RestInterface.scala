package io.elegans.orac

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.Route
import io.elegans.orac.resources._
import io.elegans.orac.services._

trait Resources extends RootAPIResource with SystemIndexManagementResource with IndexManagementResource
  with UserResource with ActionResource with ItemResource with RecommendationResource
  with RecommendationHistoryResource
  with OracUserResource
  with ForwardResource

trait RestInterface extends Resources {
  implicit def executionContext: ExecutionContext

  lazy val initActionService = ActionService
  lazy val initOracUserService = OracUserService
  lazy val initIndexManagementService = IndexManagementService
  lazy val initSystemIndexManagementService = SystemIndexManagementService
  lazy val initItemService = ItemService
  lazy val initRecommendationService = RecommendationService
  lazy val initUserService = UserService
  lazy val initForwardService = ForwardService
  lazy val initCronForwardEventsService = new CronForwardEventsService

  val routes: Route = LoggingEntities.logRequestAndResult(systemGetIndexesRoutes) ~
    LoggingEntities.logRequestAndResultB64(systemIndexManagementRoutes) ~
    LoggingEntities.logRequestAndResultB64(postIndexManagementCreateRoutes) ~
    LoggingEntities.logRequestAndResultB64(postIndexManagementRefreshRoutes) ~
    LoggingEntities.logRequestAndResultB64(putIndexManagementRoutes) ~
    LoggingEntities.logRequestAndResultB64(indexManagementRoutes) ~
    LoggingEntities.logRequestAndResult(postUserRoutes) ~
    LoggingEntities.logRequestAndResult(putUserRoutes) ~
    LoggingEntities.logRequestAndResult(deleteUserRoutes) ~
    LoggingEntities.logRequestAndResult(getUserRoutes) ~
    LoggingEntities.logRequestAndResult(genUserRoutes) ~
    LoggingEntities.logRequestAndResultReduced(actionRoutes) ~
    LoggingEntities.logRequestAndResult(itemRoutes) ~
    LoggingEntities.logRequestAndResult(oracUserRoutes) ~
    LoggingEntities.logRequestAndResult(recommendationRoutes) ~
    LoggingEntities.logRequestAndResult(recommendationHistoryRoutes) ~
    LoggingEntities.logRequestAndResult(userRecommendationRoutes) ~
    LoggingEntities.logRequestAndResult(forwardRoutes) ~
    LoggingEntities.logRequestAndResult(rootAPIsRoutes)

}
