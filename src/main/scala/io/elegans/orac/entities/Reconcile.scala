package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

case class Reconcile(
                    id: Option[String] = Option.empty[String],
                    old_id: String,
                    new_id: String,
                    `type`: ReconcileType.Value,
                    index: String,
                    index_suffix: String,
                    retry: Long,
                    timestamp: Option[Long] = Option.empty[Long]
                  )