package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

case class UpdateDocumentResult(index: String,
                               dtype: String,
                               id: String,
                               version: Long,
                               created: Boolean
                              )
