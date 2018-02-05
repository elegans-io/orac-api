package io.elegans.orac.services.auth

import akka.http.scaladsl.server.directives.Credentials
import io.elegans.orac.entities.{Permissions, User}

import scala.concurrent.Future

case class AuthenticatorException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

abstract class AbstractOracAuthenticator {

  def fetchUser(id: String): Future[User]

  def authenticator(credentials: Credentials): Future[Option[User]]

  def hasPermissions(user: User, index: String, permission: Permissions.Value): Future[Boolean]

  def secret(password: String, salt: String): String

  def hashedSecret(password: String, salt: String): String
}
