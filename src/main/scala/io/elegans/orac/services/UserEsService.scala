package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import akka.event.{Logging, LoggingAdapter}
import io.elegans.orac.entities._
import io.elegans.orac.routing.auth.{AuthenticatorException, OracAuthenticator, UserService}
import io.elegans.orac.OracActorSystem
import com.roundeights.hasher.Implicits._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io._
import javax.naming.AuthenticationException

import org.elasticsearch.action.delete.{DeleteRequestBuilder, DeleteResponse}
import org.elasticsearch.action.get.GetRequestBuilder
import org.elasticsearch.action.get.{GetResponse, MultiGetItemResponse, MultiGetRequestBuilder, MultiGetResponse}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings._
import org.elasticsearch.common.unit._
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, InnerHitBuilder, QueryBuilder, QueryBuilders}
import org.elasticsearch.index.reindex.{BulkByScrollResponse, DeleteByQueryAction}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.SearchHit

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
class UserEsService extends UserService {
  val config: Config = ConfigFactory.load()
  val elastic_client: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val index_name: String = elastic_client.index_name + "." + elastic_client.user_index_suffix

  val admin: String = config.getString("orac.basic_http_es.admin")
  val password: String = config.getString("orac.basic_http_es.password")
  val salt: String = config.getString("orac.basic_http_es.salt")
  val admin_user = User(id = admin, password = password, salt = salt,
    permissions = Map("admin" -> Set(Permissions.admin)))

  def create(user: User): Future[IndexDocumentResult] = Future {

    if(user.id == "admin") {
      throw new AuthenticationException("admin user cannot be changed")
    }

    val builder : XContentBuilder = jsonBuilder().startObject()

    builder.field("id", user.id)
    builder.field("password", user.password)
    builder.field("salt", user.salt)

    val permissions = builder.startObject("permissions")
    user.permissions.foreach(x => {
      val array = permissions.field(x._1).startArray()
      x._2.foreach(p => { array.value(p)})
      array.endArray()
    })
    permissions.endObject()

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response = client.prepareIndex().setIndex(index_name)
      .setCreate(true)
      .setType(elastic_client.user_index_suffix)
      .setId(user.id)
      .setSource(builder).get()

    val refresh_index = elastic_client.refresh_index(index_name)
    if(refresh_index.failed_shards_n > 0) {
      throw new Exception("User : index refresh failed: (" + index_name + ")")
    }

    val doc_result: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    doc_result
  }

  def update(id: String, user: UserUpdate):
  Future[UpdateDocumentResult] = Future {

    if(id == "admin") {
      throw new AuthenticationException("admin user cannot be changed")
    }

    val builder : XContentBuilder = jsonBuilder().startObject()

    user.password match {
      case Some(t) => builder.field("password", t)
      case None => ;
    }

    user.salt match {
      case Some(t) => builder.field("salt", t)
      case None => ;
    }

    user.permissions match {
      case Some(t) =>
        val permissions = builder.startObject("permissions")
        user.permissions.getOrElse(Map.empty).foreach(x => {
          val array = permissions.field(x._1).startArray()
          x._2.foreach(p => { array.value(p)})
          array.endArray()
        })
        permissions.endObject()
      case None => ;
    }

    builder.endObject()

    val client: TransportClient = elastic_client.get_client()
    val response: UpdateResponse = client.prepareUpdate().setIndex(index_name)
      .setType(elastic_client.user_index_suffix).setId(id)
      .setDoc(builder)
      .get()

    val refresh_index = elastic_client.refresh_index(index_name)
    if(refresh_index.failed_shards_n > 0) {
      throw new Exception("User : index refresh failed: (" + index_name + ")")
    }

    val doc_result: UpdateDocumentResult = UpdateDocumentResult(index = response.getIndex,
      dtype = response.getType,
      id = response.getId,
      version = response.getVersion,
      created = response.status == RestStatus.CREATED
    )

    doc_result
  }

  def delete(id: String): Future[DeleteDocumentResult] = Future {

    if(id == "admin") {
      throw new AuthenticationException("admin user cannot be changed")
    }

    val client: TransportClient = elastic_client.get_client()
    val response: DeleteResponse = client.prepareDelete().setIndex(index_name)
      .setType(elastic_client.user_index_suffix).setId(id).get()

    val refresh_index = elastic_client.refresh_index(index_name)
    if(refresh_index.failed_shards_n > 0) {
      throw new Exception("User: index refresh failed: (" + index_name + ")")
    }

    val doc_result: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status != RestStatus.NOT_FOUND
    )

    doc_result
  }

  def read(id: String): Future[User] = Future {
    if(id == "admin") {
      admin_user
    } else {

      val client: TransportClient = elastic_client.get_client()
      val get_builder: GetRequestBuilder = client.prepareGet(index_name, elastic_client.user_index_suffix, id)

      val response: GetResponse = get_builder.get()
      val source = response.getSource.asScala.toMap

      val user_id: String = source.get("id") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val password: String = source.get("password") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val salt: String = source.get("salt") match {
        case Some(t) => t.asInstanceOf[String]
        case None => ""
      }

      val permissions: Map[String, Set[Permissions.Value]] = source.get("permissions") match {
        case Some(t) => t.asInstanceOf[java.util.HashMap[String, java.util.List[String]]].asScala.map(x => {
          (x._1, x._2.asScala.map(p => Permissions.getValue(p)).toSet)
        }).toMap
        case None => Map.empty[String, Set[Permissions.Value]]
      }

      User(id = user_id, password = password, salt = salt, permissions = permissions)
    }
  }

  /** given id and optionally password and permissions, generate a new user */
  def genUser(id: String, user: UserUpdate, authenticator: OracAuthenticator): Future[User] = Future {

    val password_plain = user.password match {
      case Some(t) => t
      case None =>
        generate_password()
    }

    val salt = user.salt match {
      case Some(t) => t
      case None =>
        generate_salt()
    }

    val password = authenticator.hashed_secret(password = password_plain, salt = salt)

    val permissions = user.permissions match {
      case Some(t) => t
      case None =>
        Map.empty[String, Set[Permissions.Value]]
    }

    User(id = id, password = password, salt = salt, permissions = permissions)
  }

  def generate_password(size: Int = 16): String = {
    RandomNumbers.getString(size)
  }

  def generate_salt(): String = {
    RandomNumbers.getString(16)
  }
}