package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class UpdateAction (
  name: Option[String], /** the name of an action */
  user_id: Option[String],
  item_id: Option[String],
  timestamp: Option[Long]
)

