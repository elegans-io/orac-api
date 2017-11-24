package io.elegans.orac.routing.auth

case class AuthenticatorClassNotFoundException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

object SupportedAuthImpl extends Enumeration {
  type Permission = Value
  val basic_http_es, unknown = Value
  def getValue(auth_method: String) = values.find(_.toString == auth_method).getOrElse(SupportedAuthImpl.unknown)
}

object AuthenticatorFactory {
  def apply(auth_method: SupportedAuthImpl.Value): OracAuthenticator = {
    auth_method match {
      case SupportedAuthImpl.basic_http_es =>
        new BasicHttpOracAuthenticatorElasticSearch
      case _ =>
        throw AuthenticatorClassNotFoundException("Authenticator not supported: " + auth_method)
    }
  }
}