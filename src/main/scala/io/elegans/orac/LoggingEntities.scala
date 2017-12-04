package io.elegans.orac.resources

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import akka.event.Logging
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.http.scaladsl.server.directives.DebuggingDirectives
import akka.http.scaladsl.server.directives.LogEntry
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.server.RouteResult
import java.util.Base64

object LoggingEntities {
  val config: Config = ConfigFactory.load()

  def requestMethodAndResponseStatusReduced(req: HttpRequest): RouteResult => Option[LogEntry] = {
    case RouteResult.Complete(res) =>
      Some(LogEntry("ReqUri(" + req.uri + ") ReqMethodRes(" + req.method.name + ":" + res.status + ")",
        Logging.InfoLevel))
    case _ ⇒ None
  }

  def requestMethodAndResponseStatus(req: HttpRequest): RouteResult => Option[LogEntry] = {
    case RouteResult.Complete(res) =>
      Some(LogEntry("ReqUri(" + req.uri + ")" +
        " ReqMethodRes(" + req.method.name + ":" + res.status + ")" +
        " ReqEntity(" + req.entity.httpEntity + ") ResEntity(" + res.entity + ") "
        , Logging.InfoLevel))
    case _ ⇒ None
  }

  def requestMethodAndResponseStatusB64(req: HttpRequest): RouteResult => Option[LogEntry] = {
    case RouteResult.Complete(res) =>
      Some(LogEntry("ReqUri(" + req.uri + ")" +
        " ReqMethodRes(" + req.method.name + ":" + res.status + ")" +
        " ReqEntity(" + req.entity + ")" +
        " ReqB64Entity(" + Base64.getEncoder.encodeToString(req.entity.toString.getBytes) + ")" +
        " ResEntity(" + res.entity + ")" +
        " ResB64Entity(" + Base64.getEncoder.encodeToString(res.entity.toString.getBytes) + ")", Logging.InfoLevel))
    case _ ⇒ None
  }

  val logRequestAndResult = DebuggingDirectives.logRequestResult(requestMethodAndResponseStatus _)
  val logRequestAndResultB64 = DebuggingDirectives.logRequestResult(requestMethodAndResponseStatusB64 _)
  val logRequestAndResultReduced = DebuggingDirectives.logRequestResult(requestMethodAndResponseStatusReduced _)

}



