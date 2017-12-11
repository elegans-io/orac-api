package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 11/12/17.
  */

case class UpdateReconcile(
                      old_id: Option[String] = Option.empty[String],
                      new_id: Option[String] = Option.empty[String],
                      `type`: Option[ReconcileType.Value] = Option.empty[ReconcileType.Value],
                      index: Option[String] = Option.empty[String],
                      index_suffix: Option[String] = Option.empty[String],
                      retry: Option[Long] = Option.empty[Long],
                      timestamp: Option[Long] = Option.empty[Long]
                    )