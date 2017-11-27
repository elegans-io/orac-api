package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import io.elegans.orac.entities.{IndexManagementResponse, _}

import scala.concurrent.{Future}
import org.elasticsearch.client.transport.TransportClient
import scala.io.Source
import java.io._
import scala.collection.JavaConverters._

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.OracActorSystem
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse
import org.elasticsearch.common.xcontent.XContentType

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
object SystemIndexManagementService {
  val elastic_client: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)

  val schemaFiles: List[JsonIndexFiles] = List[JsonIndexFiles](
    JsonIndexFiles(path = "/index_management/json_index_spec/system/user.json",
      update_path = "/index_management/json_index_spec/system/update/user.json",
      index_suffix = elastic_client.user_index_suffix)
  )

  def create_index() : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elastic_client.get_client()

    val operations_message: List[String] = schemaFiles.map(item => {
      val json_in_stream: InputStream = getClass.getResourceAsStream(item.path)
      if (json_in_stream == null) {
        val message = "Check the file: (" + item.path + ")"
        throw new FileNotFoundException(message)
      }

      val schema_json: String = Source.fromInputStream(json_in_stream, "utf-8").mkString
      val full_index_name = elastic_client.index_name + "." + item.index_suffix

      val create_index_res: CreateIndexResponse =
        client.admin().indices().prepareCreate(full_index_name)
          .setSource(schema_json, XContentType.JSON).get()

      item.index_suffix + "(" + full_index_name + ", " + create_index_res.isAcknowledged.toString + ")"
    })

    val message = "IndexCreation: " + operations_message.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def remove_index() : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elastic_client.get_client()

    if (! elastic_client.enable_delete_index) {
      val message: String = "operation is not allowed, contact system administrator"
      throw new Exception(message)
    }

    val operations_message: List[String] = schemaFiles.map(item => {
      val full_index_name = elastic_client.index_name + "." + item.index_suffix

      val delete_index_res: DeleteIndexResponse =
        client.admin().indices().prepareDelete(full_index_name).get()

      item.index_suffix + "(" + full_index_name + ", " + delete_index_res.isAcknowledged.toString + ")"

    })

    val message = "IndexDeletion: " + operations_message.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def check_index() : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elastic_client.get_client()

    val operations_message: List[String] = schemaFiles.map(item => {
      val full_index_name = elastic_client.index_name + "." + item.index_suffix
      val get_mappings_req = client.admin.indices.prepareGetMappings(full_index_name).get()
      val check = get_mappings_req.mappings.containsKey(full_index_name)
      item.index_suffix + "(" + full_index_name + ", " + check + ")"
    })

    val message = "IndexCheck: " + operations_message.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def update_index() : Future[Option[IndexManagementResponse]] = Future {
    val client: TransportClient = elastic_client.get_client()

    val operations_message: List[String] = schemaFiles.map(item => {
      val json_in_stream: InputStream = getClass.getResourceAsStream(item.update_path)

      if (json_in_stream == null) {
        val message = "Check the file: (" + item.path + ")"
        throw new FileNotFoundException(message)
      }

      val schema_json: String = Source.fromInputStream(json_in_stream, "utf-8").mkString
      val full_index_name = elastic_client.index_name + "." + item.index_suffix

      val update_index_res  =
        client.admin().indices().preparePutMapping().setIndices(full_index_name)
          .setType(item.index_suffix)
          .setSource(schema_json, XContentType.JSON).get()

      item.index_suffix + "(" + full_index_name + ", " + update_index_res.isAcknowledged.toString + ")"
    })

    val message = "IndexUpdate: " + operations_message.mkString(" ")

    Option { IndexManagementResponse(message) }
  }

  def refresh_indexes() : Future[Option[RefreshIndexResults]] = Future {
    val client: TransportClient = elastic_client.get_client()

    val operations_results: List[RefreshIndexResult] = schemaFiles.map(item => {
      val full_index_name = elastic_client.index_name + "." + item.index_suffix
      val refresh_index_res: RefreshIndexResult = elastic_client.refresh_index(full_index_name)
      if (refresh_index_res.failed_shards_n > 0) {
        val index_refresh_message = item.index_suffix + "(" + full_index_name + ", " + refresh_index_res.failed_shards_n + ")"
        throw new Exception(index_refresh_message)
      }

      refresh_index_res
    })

    Option { RefreshIndexResults(results = operations_results) }
  }

  def get_indices: Future[List[String]] = Future {
    val indices_res = elastic_client.get_client
      .admin.cluster.prepareState.get.getState.getMetaData.getIndices.asScala
    indices_res.map(x => x.key).toList
  }


}
