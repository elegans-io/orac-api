package io.elegans.orac.services.auth

import com.typesafe.config.{Config, ConfigFactory}
import io.elegans.orac.services.UserService

object OracAuthenticator {
  val config: Config = ConfigFactory.load ()
  val auth_method_string: String = config.getString ("orac.auth_method")
  val auth_method: SupportedAuthImpl.Value = SupportedAuthImpl.getValue (auth_method_string)
  val auth_credential_store_string: String = config.getString ("orac.auth_credential_store")
  val auth_credential_store: SupportedAuthImpl.Value = SupportedAuthImpl.getValue (auth_credential_store_string)
  val authenticator = AuthenticatorFactory.apply(auth_method = auth_method, userService = UserService.service)
}