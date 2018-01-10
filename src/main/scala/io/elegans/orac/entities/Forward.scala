package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 30/11/17.
  */

object ForwardType extends Enumeration {
  type Forward = Value
  val item, orac_user, action, unknown = Value
  def getValue(`type`: String) = values.find(_.toString == `type`).getOrElse(unknown)
}

object ForwardOperationType extends Enumeration {
  type ForwardOperation = Value
  val create, update, delete, unknown = Value
  def getValue(`type`: String) = values.find(_.toString == `type`).getOrElse(unknown)
}

case class Forward(
                    id: Option[String] = Option.empty[String],
                    doc_id: String,
                    index: String,
                    `type`: ForwardType.Value,
                    operation: ForwardOperationType.Value,
                    retry: Option[Long] = Option{5},
                    timestamp: Option[Long] = Option.empty[Long]
                  )
