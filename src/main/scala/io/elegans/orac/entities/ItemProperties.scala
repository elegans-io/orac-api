package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class ItemProperties (
                            numerical: Option[List[NumericalProperties]],
                            string: Option[List[StringProperties]],
                            tags: Option[List[String]],
                            timestamp: Option[List[TimestampProperties]]
                          )

