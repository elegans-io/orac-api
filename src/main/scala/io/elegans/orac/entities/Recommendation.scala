package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class Recommendation(
                           id: Option[String], /** the recommendation id */
                           user_id: String,
                           item_id: String,
                           name: String, /** name of the action */
                           generation_batch: String, /** generation batch id */
                           generation_timestamp: Long,
                           score: Double /** the recommendation score */
)

case class Recommendations (
                   items: List[Recommendation]
                 )