package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 30/11/17.
  */

case class Forward(
                    id: Option[String] = Option.empty[String],
                    doc_id: String,
                    index: String,
                    index_suffix: String,
                    operation: String,
                    timestamp: Option[Long] = Option.empty[Long]
                  )
