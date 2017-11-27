package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.OracUserService
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.CircuitBreaker
import org.elasticsearch.index.engine.{DocumentMissingException, VersionConflictEngineException}

import scala.util.{Failure, Success}


trait OracUserResource extends MyResource {

  val oracUserService = OracUserService

  def oracUserRoutes: Route =
    pathPrefix("""^(index_(?:[A-Za-z0-9_]+))$""".r ~ Slash ~ "orac_user") { index_name =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = auth_realm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, index_name, Permissions.create_orac_user)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[OracUser]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(oracUserService.create(index_name, document, refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) => e match {
                        case vcee: VersionConflictEngineException =>
                          log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
                            "method=" + method.toString + " : " + vcee.getMessage)
                          completeResponse(StatusCodes.Conflict, Option.empty[String])
                        case e: Exception =>
                          log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
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
                authenticator.hasPermissions(user, index_name, Permissions.read_orac_user)) {
                extractMethod { method =>
                  parameters("ids".as[String].*) { ids =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(oracUserService.read(index_name, ids.toList)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
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
          } ~
          path(Segment) { id =>
            put {
              authenticateBasicAsync(realm = auth_realm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, index_name, Permissions.update_orac_user)) {
                  extractMethod { method =>
                    entity(as[UpdateOracUser]) { update =>
                      parameters("refresh".as[Int] ? 0) { refresh =>
                        val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                        onCompleteWithBreaker(breaker)(oracUserService.update(index_name, id, update, refresh)) {
                          case Success(t) =>
                            completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                              t
                            })
                          case Failure(e) => e match {
                            case dme: DocumentMissingException =>
                              log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
                                "method=" + method.toString + " : " + dme.getMessage)
                              completeResponse(StatusCodes.NotFound, Option.empty[String])
                            case e: Exception =>
                              log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
                                "method=" + method.toString + " : " + e.getMessage)
                              completeResponse(StatusCodes.BadRequest, Option.empty[String])
                          }
                        }
                      }
                    }
                  }
                }
              }
            } ~ delete {
              authenticateBasicAsync(realm = auth_realm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, index_name, Permissions.delete_orac_user)) {
                  extractMethod { method =>
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(oracUserService.delete(index_name, id, refresh)) {
                        case Success(t) =>
                          if (t.isDefined) {
                            completeResponse(StatusCodes.OK, t)
                          } else {
                            completeResponse(StatusCodes.BadRequest, t)
                          }
                        case Failure(e) =>
                          log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
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
}
