package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import akka.http.scaladsl.model.HttpResponse
import io.elegans.orac.entities.OracUser
import io.elegans.orac.entities.Item
import io.elegans.orac.entities.Action
import io.elegans.orac.entities.Forward

import scala.concurrent.Future

case class ForwardingException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

abstract class AbstractForwardingImplService {
  def forward_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit
  def forward_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit
  def forward_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit
}
