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
import io.elegans.orac.services.OracUserService
import org.elasticsearch.index.engine.{DocumentMissingException, VersionConflictEngineException}

import scala.util.{Failure, Success}


trait OracUserResource extends OracResource {

  private[this] val oracUserService: OracUserService.type = OracUserService

  def oracUserStreamRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix(
      """^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~
        """stream""" ~ Slash ~
        """orac_user""") { indexName =>
      pathEnd {
        get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.read_stream_orac_user)) {
              extractRequest { req =>
                if (req.entity.contentLengthOption.contains(0L)) {
                  val entryIterator = oracUserService.allDocuments(indexName)
                  val entries: Source[OracUser, NotUsed] =
                    Source.fromIterator(() => entryIterator)
                  complete(entries)
                } else {
                  entity(as[Option[OracUserSearch]]) { document =>
                    val entryIterator = oracUserService.allDocuments(indexName, document)
                    val entries: Source[OracUser, NotUsed] =
                      Source.fromIterator(() => entryIterator)
                    complete(entries)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def oracUserRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ "orac_user") { indexName =>
      pathEnd {
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.create_orac_user)) {
              extractMethod { method =>
                parameters("refresh".as[Int] ? 0) { refresh =>
                  entity(as[OracUser]) { document =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(oracUserService.create(indexName, document, refresh)) {
                      case Success(t) =>
                        completeResponse(StatusCodes.Created, StatusCodes.BadRequest, Option {
                          t
                        })
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
        } ~ get {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.read_orac_user)) {
              extractMethod { method =>
                parameters("id".as[String].*) { id =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(oracUserService.read(indexName, id.toList)) {
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
                authenticator.hasPermissions(user, indexName, Permissions.update_orac_user)) {
                extractMethod { method =>
                  entity(as[UpdateOracUser]) { update =>
                    parameters("refresh".as[Int] ? 0) { refresh =>
                      val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                      onCompleteWithBreaker(breaker)(oracUserService.update(indexName, id, update, refresh)) {
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
          } ~ delete {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.delete_orac_user)) {
                extractMethod { method =>
                  parameters("refresh".as[Int] ? 0) { refresh =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(oracUserService.delete(indexName, id, refresh)) {
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
