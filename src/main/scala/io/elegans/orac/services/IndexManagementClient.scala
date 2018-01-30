package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/03/17.
  */

object IndexManagementClient extends ElasticClient {
  val itemIndexSuffix: String = config.getString("es.item_index_suffix")
  val itemInfoIndexSuffix: String = config.getString("es.item_info_index_suffix")
  val userIndexSuffix: String = config.getString("es.user_index_suffix")
  val actionIndexSuffix: String = config.getString("es.action_index_suffix")
  val recommendationIndexSuffix: String = config.getString("es.recommendation_index_suffix")
  val recommendationHistoryIndexSuffix: String = config.getString("es.recommendation_history_index_suffix")
  val enableDeleteIndex: Boolean = config.getBoolean("es.enable_delete_application_index")
}
