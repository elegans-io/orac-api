package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 17/11/17.
  */

import scalaz.Scalaz._

object Permissions extends Enumeration {
  type Permission = Value
  val admin,
  read_stream_item, read_stream_action, read_stream_orac_user, read_stream_recomm, read_stream_recomm_history,
  create_action, update_action, read_action, delete_action,
  create_item, update_item, read_item, delete_item,
  create_orac_user, update_orac_user, read_orac_user, delete_orac_user,
  create_recomm, update_recomm, read_recomm, delete_recomm,
  create_recomm_history, update_recomm_history, read_recomm_history, delete_recomm_history,
  create_forward, read_forward, delete_forward,
  unknown = Value
  def getValue(permission: String): Permission = values.find(_.toString === permission).getOrElse(unknown)
}

