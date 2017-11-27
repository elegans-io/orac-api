package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.ItemService
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.CircuitBreaker
import org.elasticsearch.index.engine.VersionConflictEngineException

import scala.util.{Failure, Success, Try}


trait ItemResource extends MyResource {

  val itemService = ItemService

  def itemRoutes: Route =
    pathPrefix("""^(index_(?:[A-Za-z0-9_]+))$""".r ~ Slash ~ """item""") { index_name =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = auth_realm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, index_name, Permissions.create_item)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[Item]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(itemService.create(index_name, document, refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) => e match {
                        case vcee: VersionConflictEngineException =>
                          log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
                            "method=" + method.toString + " : " + e.getMessage)
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
                authenticator.hasPermissions(user, index_name, Permissions.read_item)) {
                extractMethod { method =>
                  parameters("ids".as[String].*) { ids =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(itemService.read(index_name, ids.toList)) {
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
          }
      } ~
        path(Segment) { id =>
          put {
            authenticateBasicAsync(realm = auth_realm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, index_name, Permissions.update_item)) {
                extractMethod { method =>
                  entity(as[UpdateItem]) { update =>
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(itemService.update(index_name, id, update, refresh)) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                            t
                          })
                        case Failure(e) =>
                          log.error(this.getClass.getCanonicalName + " index(" + index_name + ")" +
                            "method=" + method.toString + " : " + e.getMessage)
                          completeResponse(StatusCodes.BadRequest,
                            Option {
                              ReturnMessageData(code = 104, message = e.getMessage)
                            })
                      }
                    }
                  }
                }
              }
            }
          } ~
            delete {
              authenticateBasicAsync(realm = auth_realm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, index_name, Permissions.delete_item)) {
                  extractMethod { method =>
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(itemService.delete(index_name, id, refresh)) {
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

