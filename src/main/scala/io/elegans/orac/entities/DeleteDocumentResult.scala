package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

case class DeleteDocumentResult(id: String,
                                version: Long,
                                found: Boolean
                               )

  