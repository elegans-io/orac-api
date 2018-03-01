package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 11/12/17.
  */

case class UpdateReconcile(
                            old_id: Option[String] = Option.empty[String],
                            new_id: Option[String] = Option.empty[String],
                            item_type: Option[ReconcileType.Reconcile] = Option.empty[ReconcileType.Reconcile],
                            index: Option[String] = Option.empty[String],
                            retry: Option[Long] = Option.empty[Long],
                            timestamp: Option[Long] = Option.empty[Long]
                    )