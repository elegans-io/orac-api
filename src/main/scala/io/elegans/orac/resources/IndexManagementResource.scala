package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Route
import akka.pattern.CircuitBreaker
import io.elegans.orac.entities._
import io.elegans.orac.routing._
import io.elegans.orac.services.IndexManagementService

import scala.concurrent.duration._
import scala.util.{Failure, Success}

trait IndexManagementResource extends OracResource {

  def postIndexManagementCreateRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash
      ~ "index_management" ~ Slash ~ """create""") {
      (indexName) =>
        val indexManagementService = IndexManagementService
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, "admin", Permissions.admin)) {
              parameters("indexSuffix".as[Option[String]] ? Option.empty[String]) { indexSuffix =>
                val breaker: CircuitBreaker = OracCircuitBreaker
                  .getCircuitBreaker(callTimeout = 20.seconds)
                onCompleteWithBreaker(breaker)(
                  indexManagementService.createIndex(indexName = indexName, indexSuffix = indexSuffix)
                ) {
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

  def postIndexManagementOpenCloseRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~
      Slash ~ "index_management" ~ Slash ~ """(open|close)""".r) {
      (indexName, operation) =>
        val indexManagementService = IndexManagementService
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.admin)) {
              parameters("indexSuffix".as[Option[String]] ? Option.empty[String]) { indexSuffix =>
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(
                  indexManagementService.openCloseIndex(indexName = indexName, indexSuffix = indexSuffix,
                    operation = operation)
                ) {
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

  def postIndexManagementRefreshRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~
      Slash ~ "index_management" ~ Slash ~ """refresh""") {
      (indexName) =>
        val indexManagementService = IndexManagementService
        post {
          authenticateBasicAsync(realm = authRealm,
            authenticator = authenticator.authenticator) { user =>
            authorizeAsync(_ =>
              authenticator.hasPermissions(user, indexName, Permissions.admin)) {
              parameters("indexSuffix".as[Option[String]] ? Option.empty[String]) { indexSuffix =>
                val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                onCompleteWithBreaker(breaker)(
                  indexManagementService.refreshIndexes(indexName = indexName, indexSuffix = indexSuffix)
                ) {
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

  def putIndexManagementRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r
      ~ Slash ~ "index_management" ~ Slash ~ """(mappings|settings)""".r) {
      (indexName, mappingOrSettings) =>
        val indexManagementService = IndexManagementService
        pathEnd {
          put {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                parameters("indexSuffix".as[Option[String]] ? Option.empty[String]) { indexSuffix =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(
                    mappingOrSettings match {
                      case "mappings" => indexManagementService.updateIndexMappings(indexName = indexName,
                        indexSuffix = indexSuffix)
                      case "settings" => indexManagementService.updateIndexSettings(indexName = indexName,
                        indexSuffix = indexSuffix)
                    }
                  ) {
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
  }

  def indexManagementRoutes: Route = handleExceptions(routesExceptionHandler) {
    pathPrefix("""^(index_(?:[a-z]{1,256})_(?:[A-Za-z0-9_]{1,256}))$""".r ~ Slash ~ "index_management") {
      (indexName) =>
        val indexManagementService = IndexManagementService
        pathEnd {
          get {
            authenticateBasicAsync(realm = authRealm,
              authenticator = authenticator.authenticator) { user =>
              authorizeAsync(_ =>
                authenticator.hasPermissions(user, indexName, Permissions.admin)) {
                parameters("indexSuffix".as[Option[String]] ? Option.empty[String]) { indexSuffix =>
                  val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                  onCompleteWithBreaker(breaker)(
                    indexManagementService.checkIndex(indexName = indexName, indexSuffix = indexSuffix)
                  ) {
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
          } ~
            delete {
              authenticateBasicAsync(realm = authRealm,
                authenticator = authenticator.authenticator) { (user) =>
                authorizeAsync(_ =>
                  authenticator.hasPermissions(user, "admin", Permissions.admin)) {
                  parameters("indexSuffix".as[Option[String]] ? Option.empty[String]) { indexSuffix =>
                    val breaker: CircuitBreaker = OracCircuitBreaker.getCircuitBreaker()
                    onCompleteWithBreaker(breaker)(
                      indexManagementService.removeIndex(indexName = indexName, indexSuffix = indexSuffix)
                    ) {
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
  }
}
