package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

case class FailedShard(index_name: String,
                       shard_id: Int,
                       reason: String,
                       status: Int
                       )

case class RefreshIndexResult(index_name: String,
                             failed_shards_n: Int,
                             successful_shards_n: Int,
                             total_shards_n: Int,
                             failed_shards: List[FailedShard]
                             )

case class RefreshIndexResults(results: List[RefreshIndexResult])
