package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 2/12/17.
  */

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.ForwardService
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.CircuitBreaker
import io.elegans.orac.OracActorSystem
import org.elasticsearch.index.engine.{DocumentMissingException, VersionConflictEngineException}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

trait ForwardResource extends MyResource {

  val forwardService = ForwardService

  def forwardRoutes: Route =
    pathPrefix("""forward""") {
      pathEnd {
        post {
          authenticateBasicAsync(realm = auth_realm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[Forward]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(forwardService.create(document, refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created)
                      case Failure(e) => e match {
                        case vcee: VersionConflictEngineException =>
                          log.error(this.getClass.getCanonicalName + " index(" + forwardService.getIndexName + ")" +
                            "method=" + method.toString + " : " + vcee.getMessage)
                          completeResponse(StatusCodes.Conflict, Option.empty[String])
                        case e: Exception =>
                          log.error(this.getClass.getCanonicalName + " index(" + forwardService.getIndexName + ")" +
                            "method=" + method.toString + " : " + e.getMessage)
                          completeResponse(StatusCodes.BadRequest, Option.empty[String])
                      }
                    }
                  }
                }
              }
            }
          }
        } ~
          get {
            authenticateBasicAsync(realm = auth_realm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                extractMethod { method =>
                  parameters("id".as[String].*) { id =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(forwardService.read(id.toList)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + forwardService.getIndexName + ")" +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.BadRequest,
                          Option {
                            ReturnMessageData(code = 101, message = e.getMessage)
                          })
                    }
                  }
                }
              }
            }
          }
      } ~
        path(Segment) { id =>
          delete {
            authenticateBasicAsync(realm = auth_realm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                extractMethod { method =>
                  parameters("refresh".as[Int] ? 0) { refresh =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(forwardService.delete(id, refresh)) {
                      case Success(t) =>
                        if (t.isDefined) {
                          completeResponse(StatusCodes.OK, t)
                        } else {
                          completeResponse(StatusCodes.BadRequest, t)
                        }
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + forwardService.getIndexName + ")" +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.BadRequest,
                          Option {
                            ReturnMessageData(code = 105, message = e.getMessage)
                          })
                    }
                  }
                }
              }
            }
          }
        }
    }
}
