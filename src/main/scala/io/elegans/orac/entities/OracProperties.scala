package io.elegans.orac.entities


/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class OracProperties(
                            numerical: Option[Array[NumericalProperties]],
                            string: Option[Array[StringProperties]],
                            tags: Option[Array[String]],
                            geopoint: Option[Array[GeoPointProperties]],
                            timestamp: Option[Array[TimestampProperties]]
                          )

