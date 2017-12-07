package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class Action (
  id: Option[String] = Option.empty, /** if not specified it is automatically generated */
  name: String, /** the name of an action */
  creator_uid: Option[String] = Option.empty, /** system user id who has written the action, generated on insert */
  user_id: String, /** user id of the action */
  item_id: String, /** user id of the action */
  timestamp: Option[Long] = Option.empty, /** action timestamp, generated on insert */
  score: Option[Double] = Option.empty, /** score for the action */
  ref_url: Option[String] = Option.empty, /** referring url if any */
  ref_recommendation: Option[String] = Option.empty, /** referring recommedation if any */
)

case class Actions (
                   items: List[Action]
                 )