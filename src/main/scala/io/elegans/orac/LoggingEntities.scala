package io.elegans.orac

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import java.util.Base64

import akka.event.Logging
import akka.http.scaladsl.model.{HttpRequest, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Directive0, RouteResult}
import akka.http.scaladsl.server.directives.{DebuggingDirectives, LogEntry}
import com.typesafe.config.{Config, ConfigFactory}

object LoggingEntities {
  val config: Config = ConfigFactory.load()

  def address(remoteAddress: RemoteAddress): String = remoteAddress.toIP match {
    case Some(addr) => addr.ip.getHostAddress
    case _ => "unknown ip"
  }

  def requestMethodAndResponseStatusReduced(remoteAddress: RemoteAddress)
                                            (req: HttpRequest): RouteResult => Option[LogEntry] = {
    case RouteResult.Complete(res) =>
      Some(LogEntry("remoteAddress(" + address(remoteAddress) + ") ReqUri(" +
        req.uri + ") ReqMethodRes(" + req.method.name + ":" + res.status + ")",
        Logging.InfoLevel))
    case _ ⇒ None
  }

  def requestMethodAndResponseStatus(remoteAddress: RemoteAddress)
                                    (req: HttpRequest): RouteResult => Option[LogEntry] = {
    case RouteResult.Complete(res) =>
      Some(LogEntry("remoteAddress(" + address(remoteAddress) + ") ReqUri(" + req.uri + ")" +
        " ReqMethodRes(" + req.method.name + ":" + res.status + ")" +
        " ReqEntity(" + req.entity.httpEntity + ") ResEntity(" + res.entity + ") "
        , Logging.InfoLevel))
    case _ ⇒ None
  }

  def requestMethodAndResponseStatusB64(remoteAddress: RemoteAddress)
                                       (req: HttpRequest): RouteResult => Option[LogEntry] = {
    case RouteResult.Complete(res) =>
      Some(LogEntry("remoteAddress(" + address(remoteAddress) + ") ReqUri(" + req.uri + ")" +
        " ReqMethodRes(" + req.method.name + ":" + res.status + ")" +
        " ReqEntity(" + req.entity + ")" +
        " ReqB64Entity(" + Base64.getEncoder.encodeToString(req.entity.toString.getBytes) + ")" +
        " ResEntity(" + res.entity + ")" +
        " ResB64Entity(" + Base64.getEncoder.encodeToString(res.entity.toString.getBytes) + ")", Logging.InfoLevel))
    case _ ⇒ None
  }

  val logRequestAndResult: Directive0 =
    extractClientIP.flatMap { ip =>
      DebuggingDirectives.logRequestResult(requestMethodAndResponseStatus(ip) _)
    }

  val logRequestAndResultB64: Directive0 =
    extractClientIP.flatMap { ip =>
      DebuggingDirectives.logRequestResult(requestMethodAndResponseStatusB64(ip) _)
    }

  val logRequestAndResultReduced: Directive0 =
    extractClientIP.flatMap { ip =>
      DebuggingDirectives.logRequestResult(requestMethodAndResponseStatusReduced(ip) _)
    }
}



