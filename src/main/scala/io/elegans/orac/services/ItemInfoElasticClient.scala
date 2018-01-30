package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/12/16.
  */

object ItemInfoElasticClient extends ElasticClient {
  val itemInfoIndexSuffix: String = config.getString("es.item_info_index_suffix")
}
