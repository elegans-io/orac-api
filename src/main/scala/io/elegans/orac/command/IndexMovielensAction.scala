package io.elegans.orac.command

/**
  * Created by angelo on 7/12/17.
  */

import akka.http.scaladsl.model.HttpRequest
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.stream.ActorMaterializer
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.unmarshalling.Unmarshal


import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import io.elegans.orac.serializers.JsonSupport
import io.elegans.orac.entities._
import scopt.OptionParser
import breeze.io.CSVReader
import scala.util.Try

import com.roundeights.hasher.Implicits._

import scala.concurrent.Await
import scala.collection.immutable
import scala.collection.immutable.{List, Map}
import java.io.{File, FileReader, Reader, FileInputStream, InputStreamReader}
import java.util.Base64
import java.time.Instant
import java.util.Date

object IndexMovielensAction extends JsonSupport {
  private case class Params(
                             host: String = "http://localhost:8888",
                             index_name: String = "index_0",
                             path: String = "/action",
                             inputfile: String = "./u.data",
                             separator: Char = '\t',
                             timeout: Int = 60,
                             header_kv: Seq[String] = Seq.empty[String]
                           )

  private def load_data(params: Params): Iterator[Action] = {
    val questions_input_stream: Reader = new InputStreamReader(new FileInputStream(params.inputfile), "UTF-8")
    lazy val actions_entries = CSVReader.read(input = questions_input_stream, separator = params.separator,
      quote = '"', skipLines = 0).toIterator

    val header: Seq[String] = Seq("user_id", "item_id", "rating", "timestamp")
    val actions_iterator = actions_entries.zipWithIndex.map(entry => {
      val action = header.zip(entry._1).toMap
      val user_id = action("user_id")
      val item_id = action("item_id")
      val score = Option{action.getOrElse("rating", "0.0").toDouble}
      val timestamp = Some(Date.from(Instant.ofEpochSecond(action.getOrElse("timestamp", "0").toLong)).getTime)
      val action_data = Action(
        name = "rate",
        user_id = user_id,
        item_id = item_id,
        score = score,
        timestamp = timestamp
      )
      action_data
    })
    actions_iterator
  }

  private def doIndexData(params: Params) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val vecsize = 0

    val base_url = params.host + "/" + params.index_name + params.path

    val actions = load_data(params)

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
    actions.foreach(entry => {
      val entity_future = Marshal(entry).to[MessageEntity]
      val entity = Await.result(entity_future, 10.second)
      val responseFuture: Future[HttpResponse] =
        http.singleRequest(HttpRequest(
          method = HttpMethods.POST,
          uri = base_url,
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
          s"  default: ${defaultParams.index_name}")
        .action((x, c) => c.copy(index_name = x))
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
