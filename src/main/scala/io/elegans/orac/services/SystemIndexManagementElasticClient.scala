package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import scala.collection.JavaConverters._

object SystemIndexManagementElasticClient extends ElasticClient {
  val indexName: String = config.getString("es.system_index_name")
  val userIndexSuffix: String = config.getString("es.user_index_suffix")
  val forwardIndexSuffix: String = config.getString("es.forward_index_suffix")
  val reconcileIndexSuffix: String = config.getString("es.reconcile_index_suffix")
  val reconcileHistoryIndexSuffix: String = config.getString("es.reconcile_history_index_suffix")
  val enableDeleteIndex: Boolean = config.getBoolean("es.enable_delete_system_index")
  val forwarding: Map[String, List[(String, String, String)]] = config.getConfigList("orac.forwarding")
    .asScala.map(item => {
    (item.getString("index"), item.getString("url"), item.getString("service_type"), item.getString("item_info_id"))
  }).groupBy(_._1).mapValues(x => x.toList.map(i => (i._2, i._3, i._4)))
  val authMethod: String = config.getString("orac.auth_method")
}