package io.elegans.orac

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 22/11/17.
  */

import scala.concurrent.duration._
import akka.util.Timeout
import java.io.InputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

import akka.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext, server}
import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import akka.http.scaladsl.server

import scala.concurrent.ExecutionContextExecutor

case class Parameters(
                       http_enable: Boolean,
                       http_host: String,
                       http_port: Int,
                       https_enable: Boolean,
                       https_host: String,
                       https_port: Int,
                       https_certificate: String,
                       https_cert_pass: String)

class OracService(parameters: Option[Parameters] = None) extends RestInterface {

  val params: Option[Parameters] = if(parameters.nonEmpty) {
    parameters
  } else {
    Option {
      Parameters(
        http_enable = config.getBoolean("http.enable"),
        http_host = config.getString("http.host"),
        http_port = config.getInt("http.port"),
        https_enable = config.getBoolean("https.enable"),
        https_host = config.getString("https.host"),
        https_port = config.getInt("https.port"),
        https_certificate = config.getString("https.certificate"),
        https_cert_pass = config.getString("https.password")
      )
    }
  }

  if(params.isEmpty) {
    log.error("cannot read parameters")
  }
  assert(params.nonEmpty)

  /* creation of the akka actor system which handle concurrent requests */
  implicit val system: ActorSystem = OracActorSystem.system

  /* "The Materializer is a factory for stream execution engines, it is the thing that makes streams run" */
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val timeout: Timeout = Timeout(10.seconds)

  val api: server.Route = routes

  if (params.get.https_enable) {
    val password: Array[Char] = params.get.https_cert_pass.toCharArray

    val ks: KeyStore = KeyStore.getInstance("PKCS12")

    val keystore_path: String = "/tls/certs/" + params.get.https_certificate
    val keystore: InputStream = getClass.getResourceAsStream(keystore_path)
    //val keystore: InputStream = getClass.getClassLoader.getResourceAsStream(keystore_path)

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    val https: HttpsConnectionContext = ConnectionContext.https(sslContext)

    Http().bindAndHandle(handler = api, interface = params.get.https_host, port = params.get.https_port,
      connectionContext = https, log = system.log) map { binding =>
      system.log.info(s"REST (HTTPS) interface bound to ${binding.localAddress}")
    } recover { case ex =>
      system.log.error(s"REST (HTTPS) interface could not bind to ${params.get.http_host}:${params.get.http_port}",
        ex.getMessage)
    }
  }

  if((! params.get.https_enable) || params.get.http_enable) {
    Http().bindAndHandle(handler = api, interface = params.get.http_host,
      port = params.get.http_port, log = system.log) map { binding =>
      system.log.info(s"REST (HTTP) interface bound to ${binding.localAddress}")
    } recover { case ex =>
      system.log.error(s"REST (HTTP) interface could not bind to ${params.get.http_host}:${params.get.http_port}",
        ex.getMessage)
    }
  }

  /* activate cron jobs for events forwarding */
  initCronForwardEventsService.reloadEvents()

  /* activate cron jobs for reconciliation */
  initCronReconcileService.reloadEvents()
}

object Main extends App {
  val oracService = new OracService
}
