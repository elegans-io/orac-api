package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 23/11/16.
  */

object RecommendationElasticClient extends ElasticClient {
  val recommendation_index_suffix: String = config.getString("es.recommendation_index_suffix")
  val recommendation_history_index_suffix: String = config.getString("es.recommendation_history_index_suffix")
}
