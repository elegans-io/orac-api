package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import akka.pattern.CircuitBreaker
import scala.concurrent.{Future}
import akka.http.scaladsl.model.StatusCodes
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

trait RootAPIResource extends MyResource {
  def rootAPIsRoutes: Route = pathPrefix("") {
    pathEnd {
      get {
        val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker(maxFailure = 2, callTimeout = 1.second)
        onCompleteWithBreaker(breaker)(Future{None}) {
          case Success(v) =>
            completeResponse(StatusCodes.OK)
          case Failure(e) =>
            log.error("route=RootRoutes method=GET: " + e.getMessage)
            completeResponse(StatusCodes.BadRequest,
              Option{ReturnMessageData(code = 100, message = e.getMessage)})
        }
      }
    }
  }
}



