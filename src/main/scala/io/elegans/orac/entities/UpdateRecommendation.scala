package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 23/11/17.
  */

case class UpdateRecommendation (
                                  user_id: Option[String] = Option.empty,
                                  item_id: Option[String] = Option.empty,
                                  name: Option[String] = Option.empty, /** name of the action */
                                  generation_batch: Option[String] = Option.empty, /** generation batch id */
                                  generation_timestamp: Option[Long] = Option.empty,
                                  score: Option[Double] = Option.empty /** the recommendation score */
                                )
