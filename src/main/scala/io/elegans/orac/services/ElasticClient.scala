package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.transport.client.PreBuiltTransportClient
import org.elasticsearch.common.settings.Settings
import java.net.InetAddress

import org.elasticsearch.common.transport.TransportAddress
import scala.collection.immutable.{List, Map}
import scala.collection.JavaConverters._
import io.elegans.orac.entities._


trait ElasticClient {
  val config: Config = ConfigFactory.load()
  val cluster_name: String = config.getString("es.cluster_name")
  val ignore_cluster_name: Boolean = config.getBoolean("es.ignore_cluster_name")

  val host_map_str : String = config.getString("es.host_map")
  val host_map : Map[String, Int] = host_map_str.split(";").map(x => x.split("=")).map(x => (x(0), (x(1)).toInt)).toMap

  val settings: Settings = Settings.builder()
    .put("cluster.name", cluster_name)
    .put("client.transport.ignore_cluster_name", ignore_cluster_name)
    .put("client.transport.sniff", false).build()

  val inet_addresses: List[TransportAddress] =
    host_map.map{ case(k,v) => new TransportAddress(InetAddress.getByName(k), v) }.toList

  var client : TransportClient = open_client()

  def open_client(): TransportClient = {
    val client: TransportClient = new PreBuiltTransportClient(settings)
      .addTransportAddresses(inet_addresses:_*)
    client
  }

  def refresh_index(index_name: String): RefreshIndexResult = {
    val refresh_res: RefreshResponse =
      client.admin().indices().prepareRefresh(index_name).get()

    val failed_shards: List[FailedShard] = refresh_res.getShardFailures.map(item => {
      val failed_shard_item = FailedShard(index_name = item.index,
        shard_id = item.shardId,
        reason = item.reason,
        status = item.status.getStatus
      )
      failed_shard_item
    }).toList

    val refresh_index_result =
      RefreshIndexResult(index_name = index_name,
        failed_shards_n = refresh_res.getFailedShards,
        successful_shards_n = refresh_res.getSuccessfulShards,
        total_shards_n = refresh_res.getTotalShards,
        failed_shards = failed_shards
      )
    refresh_index_result
  }

  def get_client(): TransportClient = {
    this.client
  }

  def close_client(client: TransportClient): Unit = {
    client.close()
  }
}

