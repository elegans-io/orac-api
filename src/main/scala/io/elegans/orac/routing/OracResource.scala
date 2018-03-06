package io.elegans.orac.routing

import java.util.concurrent.TimeoutException

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.marshalling.ToEntityMarshaller
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.{Directives, ExceptionHandler, Route}
import com.typesafe.config.{Config, ConfigFactory}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.serializers.JsonSupport
import io.elegans.orac.services.UserEsServiceException
import io.elegans.orac.services.auth.{AbstractOracAuthenticator, OracAuthenticator}

import scala.concurrent.ExecutionContext
import scala.util.control.NonFatal

trait OracResource extends Directives with JsonSupport {

  implicit def executionContext: ExecutionContext

  val defaultHeader: RawHeader = RawHeader("application", "json")
  val config: Config = ConfigFactory.load()
  val authRealm: String = config.getString("orac.auth_realm")
  val authenticator: AbstractOracAuthenticator = OracAuthenticator.authenticator
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val routesExceptionHandler = ExceptionHandler {
    case e: TimeoutException =>
      extractUri { uri =>
        log.error("uri(" + uri + ") request timeout: " + e)
        respondWithDefaultHeader(defaultHeader) {
          complete(StatusCodes.RequestTimeout)
        }
      }
    case e: UserEsServiceException =>
      extractUri { uri =>
        log.error("uri(" + uri + ") Unauthorized: " + e)
        respondWithDefaultHeader(defaultHeader) {
          complete(StatusCodes.Unauthorized)
        }
      }
    case NonFatal(e) =>
      extractUri { uri =>
        log.error("uri(" + uri + ") Internal Error: " + e)
        respondWithDefaultHeader(defaultHeader) {
          complete(StatusCodes.BadRequest)
        }
      }
  }

  def completeResponse(status_code: StatusCode): Route = {
      complete(status_code)
  }

  def completeResponse[A: ToEntityMarshaller](status_code: StatusCode, data: Option[A]): Route = {
    data match {
      case Some(t) =>
        val header = RawHeader("application", "json")
        respondWithDefaultHeader(header) {
          complete(status_code, t)
        }
      case None =>
        complete(status_code)
    }
  }

  def completeResponse[A: ToEntityMarshaller](status_code_ok: StatusCode, status_code_failed: StatusCode,
                                     data: Option[A]): Route = {
    data match {
      case Some(t) =>
        val header = RawHeader("application", "json")
        respondWithDefaultHeader(header) {
          complete(status_code_ok, t)
        }
      case None =>
        complete(status_code_failed)
    }
  }
}
