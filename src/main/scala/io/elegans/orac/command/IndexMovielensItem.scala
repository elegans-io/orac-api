package io.elegans.orac.command

/**
  * Created by angelo on 7/12/17.
  */

import java.io.{FileInputStream, InputStreamReader, Reader}
import java.text.ParseException

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.stream.ActorMaterializer
import breeze.io.CSVReader
import io.elegans.orac.entities._
import io.elegans.orac.serializers.JsonSupport
import scopt.OptionParser

import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

object IndexMovielensItem extends JsonSupport {
  private[this] case class Params(
                             host: String = "http://localhost:8888",
                             indexName: String = "index_0",
                             path: String = "/item",
                             inputfile: String = "./u.item",
                             separator: Char = '|',
                             timeout: Int = 60,
                             header_kv: Seq[String] = Seq.empty[String]
                           )

  private[this] def loadData(params: Params): Iterator[Item] = {
    val questionsInputStream: Reader = new InputStreamReader(new FileInputStream(params.inputfile), "UTF-8")
    lazy val itemsEntries = CSVReader.read(input = questionsInputStream, separator = params.separator,
      quote = '"', skipLines = 0).toIterator

    val releaseDateFormat = new java.text.SimpleDateFormat("dd-MMM-yyyy")

    val header: Seq[String] = Seq("item_id", "title", "release_date", "video_release_date", "IMDb_URL", "unknown", "Action",
      "Adventure", "Animation", "Childrens", "Comedy", "Crime", "Documentary", "Drama", "Fantasy",
      "Film-Noir", "Horror", "Musical", "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western")
    val itemsIterator = itemsEntries.zipWithIndex.map(entry => {
      val item = header.zip(entry._1).toMap
      val id = item("item_id")

      val timestampProperties = try {
        val releaseDateMillis = releaseDateFormat.parse(item("release_date")).getTime
        val releaseDate = if (releaseDateMillis > 0) {
          releaseDateMillis
        } else 0
        Option{
          Array(
            TimestampProperties(key = "release_date", value = releaseDate)
          )
        }
      } catch {
        case pe: ParseException =>
          Option.empty[Array[TimestampProperties]]
      }

      val stringProperties = Array(
        StringProperties(key = "IMDb_URL", value = item("IMDb_URL"))
      ) ++ header.drop(5).map(x => {
        val v = item(x)
        if (v == "1")
          StringProperties(key = "category", value = x)
        else
          None.orNull
      }).toArray.filter(_ != None.orNull)

      val properties = Option {
        OracProperties(string = Some(stringProperties), timestamp = timestampProperties)
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
    itemsIterator
  }

  private[this] def doIndexData(params: Params) {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = system.dispatcher

    val base_url = params.host + "/" + params.indexName + params.path

    val items = loadData(params)

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

  def main(args: Array[String]) : Unit = {
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
