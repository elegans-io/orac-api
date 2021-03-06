package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 20/12/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._

import scala.concurrent.Future
import akka.http.scaladsl.model.StatusCodes

import scala.util.{Failure, Success}
import akka.pattern.CircuitBreaker
import io.elegans.orac.services.{AbstractUserService, UserService}
import org.elasticsearch.index.engine.{DocumentMissingException, VersionConflictEngineException}


trait UserResource extends OracResource {

  private[this] val userService: AbstractUserService = UserService.service

  def postUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      post {
        authenticateBasicAsync(realm = authRealm,
          authenticator = authenticator.authenticator) { user =>
          authorizeAsync(_ =>
            authenticator.hasPermissions(user, "admin", Permissions.admin)) {
            extractMethod { method =>
              entity(as[User]) { user_entity =>
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(userService.create(user_entity)) {
                  case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                  case Failure(e) => e match {
                    case vcee: VersionConflictEngineException =>
                      log.error(this.getClass.getCanonicalName + " " +
                        "method=" + method.toString + " : " + vcee.getMessage)
                      completeResponse(StatusCodes.Conflict, Option.empty[String])
                    case e: Exception =>
                      log.error(this.getClass.getCanonicalName + " " +
                        "method=" + method.toString + " : " + e.getMessage)
                      completeResponse(StatusCodes.BadRequest, Option.empty[String])
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def putUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      path(Segment) { id =>
        put {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {

              extractMethod { method =>
                entity(as[UserUpdate]) { user_entity =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(userService.update(id, user_entity)) {
                    case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                      t
                    })
                    case Failure(e) => e match {
                      case dme: DocumentMissingException =>
                        log.error(this.getClass.getCanonicalName + " " +
                          "method=" + method.toString + " : " + dme.getMessage)
                        completeResponse(StatusCodes.NotFound, Option.empty[String])
                      case e: Exception =>
                        log.error(this.getClass.getCanonicalName + " " +
                          "method=" + method.toString + " : " + e.getMessage)
                        completeResponse(StatusCodes.BadRequest, Option.empty[String])
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

  def deleteUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      path(Segment) { id =>
        delete {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(userService.delete(id)) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                  t
                })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
                  Option {
                    ReturnMessageData(code = 102, message = e.getMessage)
                  })
              }
            }
          }
        }
      }
    }
  }

  def getUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user") {
      path(Segment) { id =>
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
              onCompleteWithBreaker(breaker)(userService.read(id)) {
                case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                  t
                })
                case Failure(e) => completeResponse(StatusCodes.BadRequest,
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

  def genUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("user_gen") {
      path(Segment) { id =>
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              entity(as[UserUpdate]) { user_entity =>
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(Future {
                  userService.genUser(id, user_entity, authenticator)
                }) {
                  case Success(t) => completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                    t
                  })
                  case Failure(e) => completeResponse(StatusCodes.BadRequest,
                    Option {
                      ReturnMessageData(code = 104, message = e.getMessage)
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
