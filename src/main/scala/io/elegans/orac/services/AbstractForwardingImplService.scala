package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import io.elegans.orac.entities._
import scala.concurrent.Future

case class ForwardingException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

abstract class AbstractForwardingImplService {
  def forward_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit
  def forward_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit
  def forward_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit
  def get_recommendations(user_id: String, from: Int = 0, size: Int = 10): Option[Array[Recommendation]]
}
