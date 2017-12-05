package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class UpdateAction (
  name: Option[String], /** the name of an action */
  creator_uid: Option[String], /** system user id who has written the action */
  user_id: Option[String],
  item_id: Option[String],
  timestamp: Option[Long],
  score: Option[Double], /** score for the action */
  ref_url: Option[String], /** referring url if any */
  ref_recommendation: Option[String], /** referring recommedation if any */
)