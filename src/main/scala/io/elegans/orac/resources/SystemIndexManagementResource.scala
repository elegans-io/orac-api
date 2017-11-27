package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import akka.http.scaladsl.model.StatusCodes
import io.elegans.orac.services.SystemIndexManagementService

import scala.util.{Failure, Success}
import akka.pattern.CircuitBreaker
import org.elasticsearch.index.engine.VersionConflictEngineException

trait SystemIndexManagementResource extends MyResource {

  val systemIndexManagementService = SystemIndexManagementService

  def systemGetIndexesRoutes: Route =
    pathPrefix("system_indices") {

      pathEnd {
        get {
          authenticateBasicAsync(realm = auth_realm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(systemIndexManagementService.get_indices) {
                case Success(t) =>
                  completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option{t})
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
                  Option {
                    ReturnMessageData(code = 100, message = e.getMessage)
                  })
              }
            }
          }
        }
      }
    }

  def systemIndexManagementRoutes: Route = pathPrefix("system_index_management") {
    path(Segment) { operation: String =>
      post {
        authenticateBasicAsync(realm = auth_realm,
          authenticator = authenticator.authenticator) { user =>
          authorizeAsync(_ =>
            authenticator.hasPermissions(user, "admin", Permissions.admin)) {
            extractMethod { method =>
              operation match {
                case "refresh" =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(systemIndexManagementService.refresh_indexes()) {
                    case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                      t
                    })
                    case Failure(e) => e match {
                      case vcee: VersionConflictEngineException =>
                        log.error(this.getClass.getCanonicalName + " " +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.Conflict, Option.empty[String])
                      case e: Exception =>
                        log.error(this.getClass.getCanonicalName + " " +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.BadRequest, Option {
                          IndexManagementResponse(message = e.getMessage)
                        })
                    }
                  }
                case "create" =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(systemIndexManagementService.create_index()) {
                    case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                      t
                    })
                    case Failure(e) => completeResponse(StatusCodes.BadRequest,
                      Option {
                        IndexManagementResponse(message = e.getMessage)
                      })
                  }
                case _ => completeResponse(StatusCodes.BadRequest,
                  Option {
                    IndexManagementResponse(message = "index(system) Operation not supported: " + operation)
                  })
              }
            }
          }
        }
      }
    } ~
      pathEnd {
        get {
          authenticateBasicAsync(realm = auth_realm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(systemIndexManagementService.check_index()) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                  t
                })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
                  Option {
                    IndexManagementResponse(message = e.getMessage)
                  })
              }
            }
          }
        } ~
          delete {
            authenticateBasicAsync(realm = auth_realm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(systemIndexManagementService.remove_index()) {
                  case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                  case Failure(e) => completeResponse(StatusCodes.BadRequest,
                    Option {
                      IndexManagementResponse(message = e.getMessage)
                    })
                }
              }
            }
          } ~
          put {
            authenticateBasicAsync(realm = auth_realm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(systemIndexManagementService.update_index()) {
                  case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                  case Failure(e) => completeResponse(StatusCodes.BadRequest,
                    Option {
                      IndexManagementResponse(message = e.getMessage)
                    })
                }
              }
            }
          }
      }
  }
}


