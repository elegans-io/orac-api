package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 23/11/17.
  */

case class UpdateRecommendation (
                                  user_id: Option[String],
                                  item_id: Option[String],
                                  name: Option[String],/** name of the action */
                                  generation_batch: Option[String], /** generation batch id */
                                  generation_timestamp: Option[Long],
                                  score: Option[Double]/** the recommendation score */
                         )
