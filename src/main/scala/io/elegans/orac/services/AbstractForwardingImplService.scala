package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import io.elegans.orac.entities._

case class ForwardingException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

abstract class AbstractForwardingImplService {
  def forwardItem(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit
  def forwardOracUser(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit
  def forwardAction(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit
  def getRecommendations(userId: String, from: Int = 0, size: Int = 10): Option[Array[Recommendation]]
}
