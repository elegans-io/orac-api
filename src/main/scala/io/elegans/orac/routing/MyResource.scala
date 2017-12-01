package io.elegans.orac.routing

import akka.event.{Logging, LoggingAdapter}
import akka.http.scaladsl.marshalling.ToEntityMarshaller

import scala.concurrent.ExecutionContext
import akka.http.scaladsl.server.Directives
import io.elegans.orac.serializers.JsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.server.Route
import io.elegans.orac.OracActorSystem
import io.elegans.orac.services.auth.{AbstractOracAuthenticator, AuthenticatorFactory, OracAuthenticator, SupportedAuthImpl}
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

trait MyResource extends Directives with JsonSupport {

  implicit def executionContext: ExecutionContext

  val config: Config = ConfigFactory.load()
  val auth_realm: String = config.getString("orac.auth_realm")
  val authenticator = OracAuthenticator.authenticator
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

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
