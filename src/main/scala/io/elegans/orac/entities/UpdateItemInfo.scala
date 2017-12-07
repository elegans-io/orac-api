package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/12/17.
  */

case class UpdateItemInfo(
                     base_fields: Option[Set[String]],
                     tag_filters: Option[String],
                     numerical_filters: Option[String],
                     string_filters: Option[String],
                     timestamp_filters: Option[String],
                     geopoint_filters: Option[String]
                   )