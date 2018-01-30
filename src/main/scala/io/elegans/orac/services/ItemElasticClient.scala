package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/16.
  */

object ItemElasticClient extends ElasticClient {
  val itemIndexSuffix: String = config.getString("es.item_index_suffix")
}
