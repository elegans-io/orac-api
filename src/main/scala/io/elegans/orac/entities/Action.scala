package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class Action (
  id: Option[String], /** if not specified it is automatically generated */
  name: String, /** the name of an action */
  user_id: String,
  item_id: String,
  timestamp: Long
)

case class Actions (
                   items: List[Action]
                 )