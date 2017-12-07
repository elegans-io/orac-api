package io.elegans.orac.entities


/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class OracProperties(
                            numerical: Option[Array[NumericalProperties]] = Option.empty,
                            string: Option[Array[StringProperties]] = Option.empty,
                            tags: Option[Array[String]] = Option.empty,
                            geopoint: Option[Array[GeoPointProperties]] = Option.empty,
                            timestamp: Option[Array[TimestampProperties]] = Option.empty
                          )

