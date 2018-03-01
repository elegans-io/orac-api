package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

case class ReconcileHistory(
                             id: Option[String] = Option.empty[String],
                             old_id: String,
                             new_id: String,
                             item_type: ReconcileType.Reconcile,
                             index: Option[String] = Option.empty,
                             insert_timestamp: Long,
                             retry: Long,
                             end_timestamp: Option[Long] = Option.empty[Long]
                    )