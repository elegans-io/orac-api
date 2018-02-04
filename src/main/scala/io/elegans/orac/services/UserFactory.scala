package io.elegans.orac.services

import scalaz.Scalaz._

case class UserClassNotFoundException(message: String = "", cause: Throwable = None.orNull  )
  extends Exception(message, cause)

object SupportedAuthCredentialStoreImpl extends Enumeration {
  type Permission = Value
  val es, unknown = Value
  def getValue(authMethod: String): SupportedAuthCredentialStoreImpl.Value =
    values.find(_.toString === authMethod).getOrElse(SupportedAuthCredentialStoreImpl.unknown)
}

object UserFactory {
  def apply(userCredentialStore: SupportedAuthCredentialStoreImpl.Value): AbstractUserService = {
    userCredentialStore match {
      case SupportedAuthCredentialStoreImpl.es =>
        new UserEsService
      case _ =>
        throw UserClassNotFoundException("User service credentials store not supported: " + userCredentialStore)
    }
  }
}