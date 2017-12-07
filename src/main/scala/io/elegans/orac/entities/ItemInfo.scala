package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 6/12/17.
  */

case class ItemInfo(
                     id: String,
                     base_fields: Set[String],
                     tag_filters: String,
                     numerical_filters: String,
                     string_filters: String,
                     timestamp_filters: String,
                     geopoint_filters: String
                   )

case class ItemInfoRecords (
                   items: List[ItemInfo]
                 )