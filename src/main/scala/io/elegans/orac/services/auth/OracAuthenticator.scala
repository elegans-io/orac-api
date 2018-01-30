package io.elegans.orac.services.auth

import com.typesafe.config.{Config, ConfigFactory}
import io.elegans.orac.services.UserService

object OracAuthenticator {
  val config: Config = ConfigFactory.load ()
  val authMethodString: String = config.getString ("orac.auth_method")
  val authMethod: SupportedAuthImpl.Value = SupportedAuthImpl.getValue (authMethodString)
  val authCredentialStoreString: String = config.getString ("orac.auth_credential_store")
  val authCredentialStore: SupportedAuthImpl.Value = SupportedAuthImpl.getValue (authCredentialStoreString)
  val authenticator = AuthenticatorFactory.apply(auth_method = authMethod, userService = UserService.service)
}