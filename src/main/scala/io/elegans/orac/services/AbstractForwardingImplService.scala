package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

import io.elegans.orac.entities.OracUser
import io.elegans.orac.entities.Item
import io.elegans.orac.entities.Action
import io.elegans.orac.entities.Forward

abstract class AbstractForwardingImplService {
  def forward_item(forward: Forward, document: Option[Item] = Option.empty[Item]): Unit
  def forward_orac_user(forward: Forward, document: Option[OracUser] = Option.empty[OracUser]): Unit
  def forward_action(forward: Forward, document: Option[Action] = Option.empty[Action]): Unit
}
