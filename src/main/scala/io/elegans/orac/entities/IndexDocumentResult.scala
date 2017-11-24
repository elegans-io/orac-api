package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

case class IndexDocumentResult(id: String,
                    version: Long,
                    created: Boolean
                   )

case class IndexDocumentListResult(data: List[IndexDocumentResult])
