package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 01/07/16.
  */

object OracUserElasticClient extends ElasticClient {
  val orac_user_index_suffix: String = config.getString("es.orac_user_index_suffix")
}

