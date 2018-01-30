package io.elegans.orac.command

/**
  * Created by angelo on 7/12/17.
  */

import java.io.{FileInputStream, InputStreamReader, Reader}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import breeze.io.CSVReader
import io.elegans.orac.entities._
import io.elegans.orac.serializers.JsonSupport
import scopt.OptionParser

import scala.collection.immutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

object IndexMovielensUser extends JsonSupport {
  private[this] case class Params(
                             host: String = "http://localhost:8888",
                             indexName: String = "index_0",
                             path: String = "/orac_user",
                             inputfile: String = "./u.user",
                             separator: Char = '|',
                             timeout: Int = 60,
                             header_kv: Seq[String] = Seq.empty[String]
                           )

  private[this] def loadData(params: Params): Iterator[OracUser] = {
    val questionsInputStream: Reader = new InputStreamReader(new FileInputStream(params.inputfile), "UTF-8")
    lazy val itemsEntries = CSVReader.read(input = questionsInputStream, separator = params.separator,
      quote = '"', skipLines = 0).toIterator

    val header: Seq[String] = Seq("user","age","gender", "occupation", "zip_code")
    val userIterator = itemsEntries.zipWithIndex.map(entry => {
      val user = header.zip(entry._1).toMap
      val id = user("user")

      val stringProperties = Array(
        StringProperties(key = "gender", value = user("gender")),
        StringProperties(key = "occupation", value = user("occupation")),
        StringProperties(key = "zip_code", value = user("zip_code"))
      )

      val numericalProperties = Array(
        NumericalProperties(key = "age", value = user("age").toDouble)
      )

      val properties = Option {
        OracProperties(numerical = Option {numericalProperties},
          string = Option {stringProperties}
        )
      }

      val userData = OracUser(
        id = id,
        properties = properties
      )
      userData
    })
    userIterator
  }

  private[this] def doIndexData(params: Params) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val baseUrl = params.host + "/" + params.indexName + params.path

    val oracUsers = loadData(params)

    val httpHeader: immutable.Seq[HttpHeader] = if(params.header_kv.nonEmpty) {
      val headers: Seq[RawHeader] = params.header_kv.map(x => {
        val header_opt = x.split(":")
        val key = header_opt(0)
        val value = header_opt(1)
        RawHeader(key, value)
      }) ++ Seq(RawHeader("application", "json"))
      headers.to[immutable.Seq]
    } else {
      immutable.Seq(RawHeader("application", "json"))
    }

    val timeout = Duration(params.timeout, "s")

    val http = Http()
    oracUsers.foreach(entry => {
      val entity_future = Marshal(entry).to[MessageEntity]
      val entity = Await.result(entity_future, 10.second)
      val responseFuture: Future[HttpResponse] =
        http.singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = baseUrl,
          headers = httpHeader,
          entity = entity))
      val result = Await.result(responseFuture, timeout)
      result.status match {
        case StatusCodes.Created | StatusCodes.OK => println("indexed: " + entry.id)
        case _ =>
          println("failed indexing entry(" + entry + ") Message(" + result.toString() + ")")
      }
    })

    Await.ready(system.terminate(), Duration.Inf)
  }

  def main(args: Array[String]) {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("IndexMovielensUser") {
      head("Index users from movielens format")
      help("help").text("prints this usage text")
      opt[String]("path")
        .text(s"path of the orac_user REST endpoint" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[String]("inputfile")
        .text(s"path of the file with orac user to index" +
          s"  default: ${defaultParams.inputfile}")
        .action((x, c) => c.copy(inputfile = x))
      opt[String]("host")
        .text(s"*Chat base url" +
          s"  default: ${defaultParams.host}")
        .action((x, c) => c.copy(host = x))
      opt[String]("index_name")
        .text(s"the index_name, e.g. index_XXX" +
          s"  default: ${defaultParams.indexName}")
        .action((x, c) => c.copy(indexName = x))
      opt[Int]("timeout")
        .text(s"the timeout in seconds of each insert operation" +
          s"  default: ${defaultParams.timeout}")
        .action((x, c) => c.copy(timeout = x))
      opt[Seq[String]]("header_kv")
        .text(s"header key-value pair, as key1:value1,key2:value2" +
          s"  default: ${defaultParams.header_kv}")
        .action((x, c) => c.copy(header_kv = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(params) =>
        doIndexData(params)
      case _ =>
        sys.exit(1)
    }
  }
}
