package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 30/11/17.
  */

import scalaz.Scalaz._

object ForwardDefaults {
  val maxRetry: Option[Long] = Option{5}
}

object ForwardType extends Enumeration {
  type Forward = Value
  val item, orac_user, action, unknown = Value
  def getValue(forward_type: String): ForwardType.Value = values.find(_.toString === forward_type).getOrElse(unknown)
}

object ForwardOperationType extends Enumeration {
  type ForwardOperation = Value
  val create, update, delete, unknown = Value
  def getValue(item_type: String): ForwardOperationType.Value =
    values.find(_.toString === item_type).getOrElse(unknown)
}

case class Forward(
                    id: Option[String] = Option.empty[String],
                    doc_id: String,
                    index: Option[String] = Option.empty[String],
                    item_type: ForwardType.Value,
                    operation: ForwardOperationType.ForwardOperation,
                    retry: Option[Long] = ForwardDefaults.maxRetry,
                    timestamp: Option[Long] = Option.empty[Long]
                  )
