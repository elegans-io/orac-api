package io.elegans.orac.entities

case class UpdateRecommendationHistory (
                                         recommendation_id: Option[String] = Option.empty, /** recommendation id */
                                         access_user_id: Option[String] = Option.empty, /** system user id who accessed to the recommendation */
                                         user_id: Option[String] = Option.empty, /** user id (from original recommendation) */
                                         item_id: Option[String] = Option.empty, /** item id (from original recommendation) */
                                         name: Option[String] = Option.empty, /** name (from original recommendation) */
                                         algorithm: Option[String], /** algorithm name (from original recommendation) */
                                         generation_batch: Option[String] = Option.empty, /** generation batch id */
                                         generation_timestamp: Option[Long] = Option.empty, /** generation timestamp (from original recommendation) */
                                         access_timestamp: Option[Long] = Option.empty, /** timestamp of recommendation access */
                                         score: Option[Double] = Option.empty /** recommendation score (from original recommendation) */
                                       )
