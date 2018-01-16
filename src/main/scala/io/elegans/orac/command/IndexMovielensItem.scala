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
import java.text.ParseException

object IndexMovielensItem extends JsonSupport {
  private case class Params(
                             host: String = "http://localhost:8888",
                             index_name: String = "index_0",
                             path: String = "/item",
                             inputfile: String = "./u.item",
                             separator: Char = '|',
                             timeout: Int = 60,
                             header_kv: Seq[String] = Seq.empty[String]
                           )

  private def load_data(params: Params): Iterator[Item] = {
    val questions_input_stream: Reader = new InputStreamReader(new FileInputStream(params.inputfile), "UTF-8")
    lazy val items_entries = CSVReader.read(input = questions_input_stream, separator = params.separator,
      quote = '"', skipLines = 0).toIterator

    val release_date_format = new java.text.SimpleDateFormat("dd-MMM-yyyy")

    val header: Seq[String] = Seq("item_id", "title", "release_date", "video_release_date", "IMDb_URL", "unknown", "Action",
      "Adventure", "Animation", "Childrens", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
      "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western")
    val items_iterator = items_entries.zipWithIndex.map(entry => {
      val item = header.zip(entry._1).toMap
      val id = item("item_id")

      val timestamp_properties = try {
        val release_date_millis = release_date_format.parse(item("release_date")).getTime
        val release_date = if (release_date_millis > 0) {
          release_date_millis
        } else 0
        Option{
          Array(
            TimestampProperties(key = "release_date", value = release_date)
          )
        }
      } catch {
        case pe: ParseException =>
          Option.empty[Array[TimestampProperties]]
      }

      val string_properties = Array(
        StringProperties(key = "IMDb_URL", value = item("IMDb_URL"))
      ) ++ header.drop(5).map(x => {
        val v = item(x)
        if (v == "1")
          StringProperties(key = "category", value = x)
        else
          null
      }).toArray.filter(_ != null)

      val properties = Option {
        OracProperties(string = Some(string_properties), timestamp = timestamp_properties)
      }

      val name = item.getOrElse("name", item("title"))
      val `type` = item.getOrElse("type", "movie")
      val item_data = Item(
        id = id,
        name = name,
        `type` = `type`,
        properties = properties
      )
      item_data
    })
    items_iterator
  }

  private def doIndexData(params: Params) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val base_url = params.host + "/" + params.index_name + params.path

    val items = load_data(params)

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
    items.foreach(entry => {
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
    val parser = new OptionParser[Params]("IndexMovielensItem") {
      head("Index movielens items")
      help("help").text("prints this usage text")
      opt[String]("path")
        .text(s"path of the item REST endpoint" +
          s"  default: ${defaultParams.path}")
        .action((x, c) => c.copy(path = x))
      opt[String]("inputfile")
        .text(s"path of the file with items to index" +
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