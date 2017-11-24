package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

object SystemIndexManagementElasticClient extends ElasticClient {
  val index_name: String = config.getString("es.system_index_name")
  val user_index_suffix: String = config.getString("es.user_index_suffix")
  val enable_delete_index: Boolean = config.getBoolean("es.enable_delete_system_index")
  val auth_method: String = config.getString("orac.auth_method")
}