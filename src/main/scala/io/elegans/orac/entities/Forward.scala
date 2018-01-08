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
                    retry: Option[Long] = Option{5},
                    timestamp: Option[Long] = Option.empty[Long]
                  )

case class ForwardAll(
                    total_objects: Long,
                    queued_objects: Long,
                  )