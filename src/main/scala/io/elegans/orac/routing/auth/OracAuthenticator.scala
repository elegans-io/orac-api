package io.elegans.orac.routing.auth

import akka.http.scaladsl.server.directives.SecurityDirectives._

import scala.concurrent.Future
import io.elegans.orac.entities.{User, Permissions}

case class AuthenticatorException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

trait OracAuthenticator {

  def fetchUser(id: String): Future[User]

  val authenticator: AsyncAuthenticatorPF[User]

  def hasPermissions(user: User, index: String, permission: Permissions.Value): Future[Boolean]

  def secret(password: String, salt: String): String

  def hashed_secret(password: String, salt: String): String
}
