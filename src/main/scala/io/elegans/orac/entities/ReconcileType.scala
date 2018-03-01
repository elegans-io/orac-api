package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

object ReconcileType extends Enumeration {
  type Reconcile = Value
  val orac_user, unknown = Value
  def getValue(item_type: String): Reconcile = values.find(_.toString == item_type).getOrElse(unknown)
}