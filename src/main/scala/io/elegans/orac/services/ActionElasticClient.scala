package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 12/03/17.
  */

object ActionElasticClient extends ElasticClient {
  val action_index_suffix: String = config.getString("es.action_index_suffix")
}
