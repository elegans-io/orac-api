package io.elegans.orac.command

/**
  * Created by angelo on 7/12/17.
  */

import java.io.{FileInputStream, InputStreamReader, Reader}
import java.time.Instant
import java.util.Date

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.stream.ActorMaterializer
import breeze.io.CSVReader
import io.elegans.orac.entities._
import io.elegans.orac.serializers.OracApiJsonSupport
import scopt.OptionParser

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor, Future}

object IndexMovielensAction extends OracApiJsonSupport {
  private[this] case class Params(
                             host: String = "http://localhost:8888",
                             indexName: String = "index_english_0",
                             path: String = "/action",
                             inputfile: String = "./u.data",
                             separator: Char = '\t',
                             timeout: Int = 60,
                             header_kv: Seq[String] = Seq.empty[String]
                           )

  private[this] def loadData(params: Params): Iterator[Action] = {
    val dataInputStream: Reader = new InputStreamReader(new FileInputStream(params.inputfile), "UTF-8")
    lazy val actionsEntries = CSVReader.read(input = dataInputStream, separator = params.separator,
      quote = '"', skipLines = 0).toIterator

    val header: Seq[String] = Seq("user_id", "item_id", "rating", "timestamp")
    val actionsIterator = actionsEntries.zipWithIndex.map(entry => {
      val action = header.zip(entry._1).toMap
      val userId = action("user_id")
      val itemId = action("item_id")
      val score = Option{action.getOrElse("rating", "0.0").toDouble}
      val timestamp = Some(Date.from(Instant.ofEpochSecond(action.getOrElse("timestamp", "0").toLong)).getTime)
      val actionData = Action(
        name = "rate",
        user_id = userId,
        item_id = itemId,
        score = score,
        timestamp = timestamp
      )
      actionData
    })
    actionsIterator
  }

  private[this] def doIndexData(params: Params) : Unit = {
    implicit val system: ActorSystem = ActorSystem()
    implicit val materializer: ActorMaterializer = ActorMaterializer()
    implicit val executionContext: ExecutionContextExecutor = system.dispatcher

    val baseUrl = params.host + "/" + params.indexName + params.path

    val actions = loadData(params)

    val httpHeader: immutable.Seq[HttpHeader] = if(params.header_kv.nonEmpty) {
      val headers: Seq[RawHeader] = params.header_kv.map(x => {
        val headerOpt = x.split(":")
        val key = headerOpt(0)
        val value = headerOpt(1)
        RawHeader(key, value)
      }) ++ Seq(RawHeader("application", "json"))
      headers.to[immutable.Seq]
    } else {
      immutable.Seq(RawHeader("application", "json"))
    }

    val timeout = Duration(params.timeout, "s")

    val http = Http()
    actions.foreach(entry => {
      val entityFuture = Marshal(entry).to[MessageEntity]
      val entity = Await.result(entityFuture, 10.second)
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

  def main(args: Array[String]) : Unit = {
    val defaultParams = Params()
    val parser = new OptionParser[Params]("IndexMovielensActions") {
      head("Index movielens actions")
      help("help").text("prints this usage text")
      opt[String]("path")
        .text(s"path of the action REST endpoint" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[String]("inputfile")
        .text(s"path of the file with actions to index" +
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
