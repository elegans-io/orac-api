package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class UpdateOracUser(
                           name: Option[String],
                           email: Option[String],
                           phone: Option[String],
                           properties: Option[OracProperties]
)
