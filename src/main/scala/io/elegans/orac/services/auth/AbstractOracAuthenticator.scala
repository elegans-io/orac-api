package io.elegans.orac.services.auth

import akka.http.scaladsl.server.directives.Credentials

import scala.concurrent.Future
import io.elegans.orac.entities.{Permissions, User}

case class AuthenticatorException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

abstract class AbstractOracAuthenticator {

  def fetchUser(id: String): Future[User]

  def authenticator(credentials: Credentials): Future[Option[User]]

  def hasPermissions(user: User, index: String, permission: Permissions.Value): Future[Boolean]

  def secret(password: String, salt: String): String

  def hashed_secret(password: String, salt: String): String
}
