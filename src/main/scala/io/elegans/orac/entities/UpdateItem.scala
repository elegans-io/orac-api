package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class UpdateItem (
  name: Option[String],
  `type`: Option[String],
  description: Option[String],
  properties: Option[OracProperties]
)
