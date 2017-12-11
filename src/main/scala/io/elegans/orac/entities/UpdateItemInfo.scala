package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/12/17.
  */

case class UpdateItemInfo(
                     base_fields: Option[Set[String]] = Option.empty,
                     tag_filters: Option[String] = Option.empty,
                     numerical_filters: Option[String] = Option.empty,
                     string_filters: Option[String] = Option.empty,
                     timestamp_filters: Option[String] = Option.empty,
                     geopoint_filters: Option[String] = Option.empty
                   )