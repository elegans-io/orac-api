package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class Action (
  id: Option[String], /** if not specified it is automatically generated */
  name: String, /** the name of an action */
  creator_uid: Option[String], /** system user id who has written the action, generated on insert */
  user_id: String, /** user id of the action */
  item_id: String, /** user id of the action */
  timestamp: Option[Long], /** action timestamp, generated on insert */
  ref_url: Option[String], /** referring url if any */
  ref_recommendation: Option[String], /** referring recommedation if any */
)

case class Actions (
                   items: List[Action]
                 )