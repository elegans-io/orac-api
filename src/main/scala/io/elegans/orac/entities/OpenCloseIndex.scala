package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/03/18.
  */

case class OpenCloseIndex(
                           indexName: String,
                           indexSuffix: String,
                           fullIndexName: String,
                           operation: String,
                           acknowledgement: Boolean
                         )
