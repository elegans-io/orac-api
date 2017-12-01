package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import io.elegans.orac.entities._
import io.elegans.orac.services.auth.AbstractOracAuthenticator
import scala.concurrent.Future

abstract class AbstractUserService {
  def create(user: User): Future[IndexDocumentResult]
  def update(id: String, user: UserUpdate): Future[UpdateDocumentResult]
  def delete(id: String): Future[DeleteDocumentResult]
  def read(id: String): Future[User]
  def genUser(id: String, user: UserUpdate, authenticator: AbstractOracAuthenticator): Future[User]
}