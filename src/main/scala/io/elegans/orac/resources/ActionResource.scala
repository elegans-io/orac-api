package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.NotUsed
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import akka.stream.scaladsl.Source
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.ActionService
import org.elasticsearch.index.engine.{DocumentMissingException, VersionConflictEngineException}

import scala.util.{Failure, Success}

trait ActionResource extends OracResource {

  private[this] val actionService: ActionService.type = ActionService

  def actionStreamRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix(
      """^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~
        """stream""" ~ Slash ~
        """action""") { indexName =>
      pathEnd {
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.read_stream_action)) {
              val entryIterator = actionService.allDocuments(indexName)
              val entries: Source[Action, NotUsed] =
                Source.fromIterator(() => entryIterator)
              complete(entries)
            }
          }
        }
      }
    }
  }

  def actionUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~
      Slash ~ """action""" ~ Slash ~ """user""" ) { indexName =>
      path(Segment) { id =>
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.read_action)) {
              extractMethod { method =>
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                val search = Some(UpdateAction(user_id = Some(id)))
                onCompleteWithBreaker(breaker)(actionService.readAll(indexName, search)) {
                  case Success(t) =>
                    completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                      t
                    })
                  case Failure(e) =>
                    log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
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

  def actionRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ """action""") { indexName =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.create_action)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[Action]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(actionService.create(indexName, user.id, document, refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created)
                      case Failure(e) => e match {
                        case vcee: VersionConflictEngineException =>
                          log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
                            "method=" + method.toString + " : " + vcee.getMessage)
                          completeResponse(StatusCodes.Conflict, Option.empty[String])
                        case e: Exception =>
                          log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
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
                authenticator.hasPermissions(user, indexName, Permissions.read_action)) {
                extractMethod { method =>
                  parameters("id".as[String].*) { id =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(actionService.read(indexName, id.toList)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                          t
                        })
                      case Failure(e) =>
                        log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
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
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.update_action)) {
                extractMethod { method =>
                  entity(as[UpdateAction]) { update =>
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(actionService.update(indexName, id, update, refresh)) {
                        case Success(t) =>
                          completeResponse(StatusCodes.OK, StatusCodes.BadRequest, Option {
                            t
                          })
                        case Failure(e) => e match {
                          case dme: DocumentMissingException =>
                            log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
                              "method=" + method.toString + " : " + dme.getMessage)
                            completeResponse(StatusCodes.NotFound, Option.empty[String])
                          case e: Exception =>
                            log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
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
            delete {
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { user =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, indexName, Permissions.delete_action)) {
                  extractMethod { method =>
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(actionService.delete(indexName, id, refresh)) {
                        case Success(t) =>
                          if (t.isDefined) {
                            completeResponse(StatusCodes.OK, t)
                          } else {
                            completeResponse(StatusCodes.BadRequest, t)
                          }
                        case Failure(e) =>
                          log.error(this.getClass.getCanonicalName + " index(" + indexName + ") " +
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

