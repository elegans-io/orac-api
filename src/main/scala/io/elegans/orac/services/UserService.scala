package io.elegans.orac.services

import com.typesafe.config.{Config, ConfigFactory}

object UserService {
  val config: Config = ConfigFactory.load ()
  val authCredentialStoreString: String = config.getString ("orac.auth_credential_store")
  val authCredentialStore: SupportedAuthCredentialStoreImpl.Value =
    SupportedAuthCredentialStoreImpl.getValue (authCredentialStoreString)

  val service = UserFactory.apply(user_credential_store = authCredentialStore)
}