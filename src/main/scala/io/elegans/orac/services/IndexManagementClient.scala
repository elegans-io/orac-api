package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/03/17.
  */

object IndexManagementClient extends ElasticClient {
  val item_index_suffix: String = config.getString("es.item_index_suffix")
  val item_info_index_suffix: String = config.getString("es.item_info_index_suffix")
  val user_index_suffix: String = config.getString("es.user_index_suffix")
  val action_index_suffix: String = config.getString("es.action_index_suffix")
  val recommendation_index_suffix: String = config.getString("es.recommendation_index_suffix")
  val recommendation_history_index_suffix: String = config.getString("es.recommendation_history_index_suffix")
  val enable_delete_index: Boolean = config.getBoolean("es.enable_delete_application_index")
}
