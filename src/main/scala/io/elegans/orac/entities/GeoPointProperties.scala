package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 25/11/17.
  */

case class OracGeoPoint(
                      lat: Double,
                      lon: Double
                    )

case class GeoPointProperties (
                                key: String,
                                value: OracGeoPoint
                              )
