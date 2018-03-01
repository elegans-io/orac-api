package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 8/12/17.
  */

object ReconcileDefaults {
  val maxRetry: Long = 10
}

case class Reconcile(
                      id: Option[String] = Option.empty[String],
                      old_id: String,
                      new_id: String,
                      item_type: ReconcileType.Reconcile,
                      index: Option[String] = Option.empty,
                      retry: Long = ReconcileDefaults.maxRetry,
                      timestamp: Option[Long] = Option.empty[Long]
                  )