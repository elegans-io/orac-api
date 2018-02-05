package io.elegans.orac.services.auth

import io.elegans.orac.services.AbstractUserService

case class AuthenticatorClassNotFoundException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

object SupportedAuthImpl extends Enumeration {
  type Permission = Value
  val basic_http, unknown = Value
  def getValue(auth_method: String): Permission = {
    values.find(_.toString == auth_method).getOrElse(SupportedAuthImpl.unknown)
  }
}

object AuthenticatorFactory {
  def apply(auth_method: SupportedAuthImpl.Value,
            userService: AbstractUserService): AbstractOracAuthenticator = {
    auth_method match {
      case SupportedAuthImpl.basic_http =>
        new BasicHttpOracAuthenticator(userService)
      case _ =>
        throw AuthenticatorClassNotFoundException("Authenticator not supported: " + auth_method)
    }
  }
}