package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 2/12/17.
  */

import akka.http.scaladsl.server.Route
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.ReconcileService
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.CircuitBreaker
import org.elasticsearch.index.engine.VersionConflictEngineException

import scala.util.{Failure, Success}

trait ReconcileResource extends MyResource {

  val reconcileService: ReconcileService.type = ReconcileService

  def reconcileRoutes: Route =
    pathPrefix("""^(index_(?:[A-Za-z0-9_]+))$""".r ~ Slash ~ "reconcile" ~ Slash
        ~ """^(orac_user)$""".r) { (index_name, reconcile_type) =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = auth_realm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, index_name, Permissions.create_item)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[Reconcile]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(
                      reconcileService.create(document, index_name, ReconcileType.getValue(reconcile_type), refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created)
                      case Failure(e) => e match {
                        case vcee: VersionConflictEngineException =>
                          log.error(this.getClass.getCanonicalName + " index(" + reconcileService.getIndexName + ") " +
                            "method=" + method.toString + " : " + vcee.getMessage)
                          completeResponse(StatusCodes.Conflict, Option.empty[String])
                        case e: Exception =>
                          log.error(this.getClass.getCanonicalName + " index(" + reconcileService.getIndexName + ") " +
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
                authenticator.hasPermissions(user, index_name, Permissions.create_item)) {
                extractMethod { method =>
                  parameters("id".as[String].*) { id =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(reconcileService.read(id.toList, index_name)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + reconcileService.getIndexName + ") " +
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
                authenticator.hasPermissions(user, index_name, Permissions.create_item)) {
                extractMethod { method =>
                  parameters("refresh".as[Int] ? 0) { refresh =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(reconcileService.delete(id, index_name, refresh)) {
                      case Success(t) =>
                        if (t.isDefined) {
                          completeResponse(StatusCodes.OK, t)
                        } else {
                          completeResponse(StatusCodes.BadRequest, t)
                        }
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + reconcileService.getIndexName + ") " +
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
