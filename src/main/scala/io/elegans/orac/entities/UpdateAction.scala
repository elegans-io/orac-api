package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class UpdateAction (
  name: Option[String] = Option.empty, /** the name of an action */
  creator_uid: Option[String] = Option.empty, /** system user id who has written the action */
  user_id: Option[String] = Option.empty,
  item_id: Option[String] = Option.empty,
  timestamp: Option[Long] = Option.empty,
  score: Option[Double] = Option.empty, /** score for the action */
  ref_url: Option[String] = Option.empty, /** referring url if any */
  ref_recommendation: Option[String] = Option.empty, /** referring recommedation if any */
)