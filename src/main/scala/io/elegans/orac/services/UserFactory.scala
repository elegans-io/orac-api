package io.elegans.orac.services

case class UserClassNotFoundException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

object SupportedAuthCredentialStoreImpl extends Enumeration {
  type Permission = Value
  val es, unknown = Value
  def getValue(auth_method: String): SupportedAuthCredentialStoreImpl.Value =
    values.find(_.toString == auth_method).getOrElse(SupportedAuthCredentialStoreImpl.unknown)
}

object UserFactory {
  def apply(user_credential_store: SupportedAuthCredentialStoreImpl.Value): AbstractUserService = {
    user_credential_store match {
      case SupportedAuthCredentialStoreImpl.es =>
        new UserEsService
      case _ =>
        throw UserClassNotFoundException("User service credentials store not supported: " + user_credential_store)
    }
  }
}