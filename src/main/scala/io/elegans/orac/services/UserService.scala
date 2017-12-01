package io.elegans.orac.services

import com.typesafe.config.{Config, ConfigFactory}


object UserService {
  val config: Config = ConfigFactory.load ()
  val auth_credential_store_string: String = config.getString ("orac.auth_credential_store")
  val auth_credential_store: SupportedAuthCredentialStoreImpl.Value =
    SupportedAuthCredentialStoreImpl.getValue (auth_credential_store_string)

  val service = UserFactory.apply(user_credential_store = auth_credential_store)
}