package io.elegans.orac.entities


/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class OracProperties(
                            numerical: Option[List[NumericalProperties]],
                            string: Option[List[StringProperties]],
                            tags: Option[List[String]],
                            geopoint: Option[List[GeoPointProperties]],
                            timestamp: Option[List[TimestampProperties]]
                          )
