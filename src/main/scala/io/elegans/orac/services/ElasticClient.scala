package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import java.net.InetAddress

import com.typesafe.config.{Config, ConfigFactory}
import io.elegans.orac.entities._
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import scala.collection.immutable.{List, Map}

trait ElasticClient {
  val config: Config = ConfigFactory.load()
  val clusterName: String = config.getString("es.cluster_name")
  val ignoreClusterName: Boolean = config.getBoolean("es.ignore_cluster_name")

  val hostMapStr : String = config.getString("es.host_map")
  val hostMap : Map[String, Int] = hostMapStr.split(";").map(x => x.split("=")).map(x => (x(0), x(1).toInt)).toMap

  val settings: Settings = Settings.builder()
    .put("cluster.name", clusterName)
    .put("client.transport.ignore_cluster_name", ignoreClusterName)
    .put("client.transport.sniff", false).build()

  val inetAddresses: List[TransportAddress] =
    hostMap.map{ case(k,v) => new TransportAddress(InetAddress.getByName(k), v) }.toList

  var client : TransportClient = openClient()

  def openClient(): TransportClient = {
    val client: TransportClient = new PreBuiltTransportClient(settings)
      .addTransportAddresses(inetAddresses:_*)
    client
  }

  def refreshIndex(indexName: String): RefreshIndexResult = {
    val refreshRes: RefreshResponse =
      client.admin().indices().prepareRefresh(indexName).get()

    val failedShards: List[FailedShard] = refreshRes.getShardFailures.map(item => {
      val failedShardItem = FailedShard(index_name = item.index,
        shard_id = item.shardId,
        reason = item.reason,
        status = item.status.getStatus
      )
      failedShardItem
    }).toList

    val refreshIndexResult =
      RefreshIndexResult(index_name = indexName,
        failed_shards_n = refreshRes.getFailedShards,
        successful_shards_n = refreshRes.getSuccessfulShards,
        total_shards_n = refreshRes.getTotalShards,
        failed_shards = failedShards
      )
    refreshIndexResult
  }

  def getClient: TransportClient = {
    this.client
  }

  def closeClient(client: TransportClient): Unit = {
    client.close()
  }
}

