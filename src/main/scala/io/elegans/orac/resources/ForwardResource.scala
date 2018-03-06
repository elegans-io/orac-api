package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 2/12/17.
  */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.ForwardService
import org.elasticsearch.index.engine.VersionConflictEngineException

import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait ForwardResource extends OracResource {

  private[this] val forwardService: ForwardService.type = ForwardService

  def forwardAllRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ """forward_all""") { indexName =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.create_item)) {
              extractMethod { method =>
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker(callTimeout = 3600.seconds)
                onCompleteWithBreaker(breaker)(forwardService.forwardCreateAll(indexName = indexName)) {
                  case Success(t) =>
                    completeResponse(StatusCodes.OK)
                  case Failure(e) =>
                    log.error(this.getClass.getCanonicalName + " index(" + forwardService.fullIndexName + ") " +
                      "method=" + method.toString + " : " + e.getMessage)
                    completeResponse(StatusCodes.BadRequest,
                      Option {
                        ReturnMessageData(code = 100, message = e.getMessage)
                      })
                }
              }
            }
          }
        } ~
          delete {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.create_item)) {
                extractMethod { method =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker(callTimeout = 3600.seconds)
                  onCompleteWithBreaker(breaker)(forwardService.forwardDeleteAll(indexName = indexName)) {
                    case Success(t) =>
                      completeResponse(StatusCodes.OK)
                    case Failure(e) =>
                      log.error(this.getClass.getCanonicalName + " index(" + forwardService.fullIndexName + ") " +
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
    }
  }

  def forwardRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ """forward""") { indexName =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.create_item)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[Forward]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(forwardService.create(indexName, document, refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created)
                      case Failure(e) => e match {
                        case vcee: VersionConflictEngineException =>
                          log.error(this.getClass.getCanonicalName + " index(" + forwardService.fullIndexName + ") " +
                            "method=" + method.toString + " : " + vcee.getMessage)
                          completeResponse(StatusCodes.Conflict, Option.empty[String])
                        case e: Exception =>
                          log.error(this.getClass.getCanonicalName + " index(" + forwardService.fullIndexName + ") " +
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
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.create_item)) {
                extractMethod { method =>
                  parameters("id".as[String].*) { id =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(forwardService.read(indexName, id.toList)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + forwardService.fullIndexName + ") " +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.BadRequest,
                          Option {
                            ReturnMessageData(code = 102, message = e.getMessage)
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
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.create_item)) {
                extractMethod { method =>
                  parameters("refresh".as[Int] ? 0) { refresh =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(forwardService.delete(indexName, id, refresh)) {
                      case Success(t) =>
                        if (t.isDefined) {
                          completeResponse(StatusCodes.OK, t)
                        } else {
                          completeResponse(StatusCodes.BadRequest, t)
                        }
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + forwardService.fullIndexName + ") " +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.BadRequest,
                          Option {
                            ReturnMessageData(code = 103, message = e.getMessage)
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
}
