package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import javax.naming.AuthenticationException

import akka.event.{Logging, LoggingAdapter}
import com.typesafe.config.{Config, ConfigFactory}
import io.elegans.orac.OracActorSystem
import io.elegans.orac.entities._
import io.elegans.orac.tools._
import io.elegans.orac.services.auth.AbstractOracAuthenticator
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.{GetRequestBuilder, GetResponse}
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.rest.RestStatus

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz.Scalaz._

case class UserEsServiceException(message: String = "", cause: Throwable = None.orNull)
  extends Exception(message, cause)

/**
  * Implements functions, eventually used by IndexManagementResource, for ES index management
  */
class UserEsService extends AbstractUserService {
  val config: Config = ConfigFactory.load()
  val elasticClient: SystemIndexManagementElasticClient.type = SystemIndexManagementElasticClient
  val log: LoggingAdapter = Logging(OracActorSystem.system, this.getClass.getCanonicalName)
  val indexName: String = elasticClient.indexName + "." + elasticClient.userIndexSuffix

  val admin: String = config.getString("orac.basic_http_es.admin")
  val password: String = config.getString("orac.basic_http_es.password")
  val salt: String = config.getString("orac.basic_http_es.salt")
  val adminUser = User(id = admin, password = password, salt = salt,
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

    val client: TransportClient = elasticClient.openClient
    val response = client.prepareIndex().setIndex(indexName)
      .setCreate(true)
      .setType(elasticClient.userIndexSuffix)
      .setId(user.id)
      .setSource(builder).get()

    val refreshIndex = elasticClient.refreshIndex(indexName)
    if(refreshIndex.failed_shards_n > 0) {
      client.close()
      throw new Exception("User : index refresh failed: (" + indexName + ")")
    }

    val docResult: IndexDocumentResult = IndexDocumentResult(id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    client.close()
    docResult
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

    val client: TransportClient = elasticClient.openClient
    val response: UpdateResponse = client.prepareUpdate().setIndex(indexName)
      .setType(elasticClient.userIndexSuffix).setId(id)
      .setDoc(builder)
      .get()

    val refreshIndex = elasticClient.refreshIndex(indexName)
    if(refreshIndex.failed_shards_n > 0) {
      client.close()
      throw new Exception("User : index refresh failed: (" + indexName + ")")
    }

    val docResult: UpdateDocumentResult = UpdateDocumentResult(index = response.getIndex,
      dtype = response.getType,
      id = response.getId,
      version = response.getVersion,
      created = response.status === RestStatus.CREATED
    )

    client.close()
    docResult
  }

  def delete(id: String): Future[DeleteDocumentResult] = Future {

    if(id == "admin") {
      throw new AuthenticationException("admin user cannot be changed")
    }

    val client: TransportClient = elasticClient.openClient
    val response: DeleteResponse = client.prepareDelete().setIndex(indexName)
      .setType(elasticClient.userIndexSuffix).setId(id).get()

    val refreshIndex = elasticClient.refreshIndex(indexName)
    if(refreshIndex.failed_shards_n > 0) {
      client.close()
      throw new Exception("User: index refresh failed: (" + indexName + ")")
    }

    val docResult: DeleteDocumentResult = DeleteDocumentResult(id = response.getId,
      version = response.getVersion,
      found = response.status =/= RestStatus.NOT_FOUND
    )

    client.close()
    docResult
  }

  def read(id: String): Future[User] = Future {
    if(id == "admin") {
      adminUser
    } else {
      val client: TransportClient = elasticClient.openClient
      val getBuilder: GetRequestBuilder = client.prepareGet(indexName, elasticClient.userIndexSuffix, id)

      val response: GetResponse = getBuilder.get()
      val source = response.getSource.asScala.toMap

      val userId: String = source.get("id") match {
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
      client.close()
      User(id = userId, password = password, salt = salt, permissions = permissions)
    }
  }

  /** given id and optionally password and permissions, generate a new user */
  def genUser(id: String, user: UserUpdate, authenticator: AbstractOracAuthenticator): Future[User] = Future {

    val passwordPlain = user.password match {
      case Some(t) => t
      case None =>
        generatePassword()
    }

    val salt = user.salt match {
      case Some(t) => t
      case None =>
        generateSalt()
    }

    val password = authenticator.hashedSecret(password = passwordPlain, salt = salt)

    val permissions = user.permissions match {
      case Some(t) => t
      case None =>
        Map.empty[String, Set[Permissions.Value]]
    }

    User(id = id, password = password, salt = salt, permissions = permissions)
  }

  def generatePassword(size: Int = 16): String = {
    RandomNumbers.string(size)
  }

  def generateSalt(size: Int = 16): String = {
    RandomNumbers.string(size)
  }
}