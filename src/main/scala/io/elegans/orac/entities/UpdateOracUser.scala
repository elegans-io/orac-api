package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 31/10/17.
  */

case class UpdateOracUser(
                           name: Option[String] = Option.empty,
                           email: Option[String] = Option.empty,
                           phone: Option[String] = Option.empty,
                           props: Option[OracProperties] = Option.empty
)
