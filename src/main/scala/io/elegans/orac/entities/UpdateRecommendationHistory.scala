package io.elegans.orac.entities

case class UpdateRecommendationHistory (
                                   recommendation_id: Option[String], /** recommendation id */
                                   access_uid: Option[String], /** system user id who accessed to the recommendation */
                                   user_id: Option[String], /** user id (from original recommendation) */
                                   item_id: Option[String], /** item id (from original recommendation) */
                                   name: Option[String], /** name (from original recommendation) */
                                   generation_batch: Option[String], /** generation batch id */
                                   generation_timestamp: Option[Long], /** generation timestamp (from original recommendation) */
                                   access_timestamp: Option[Long],  /** timestamp of recommendation access */
                                   score: Option[Double] /** recommendation score (from original recommendation) */
                                 )
