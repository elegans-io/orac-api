package io.elegans.orac.resources

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server
import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit._
import io.elegans.orac.OracService
import io.elegans.orac.serializers.OracApiJsonSupport
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._


class RootAPIResourceTest extends WordSpec with Matchers with ScalatestRouteTest with OracApiJsonSupport {
  implicit def default(implicit system: ActorSystem): RouteTestTimeout = RouteTestTimeout(10.seconds.dilated(system))
  val service = new OracService
  val routes: server.Route = service.routes

  "StarChat" should {
    "return a 200 if the service responds (Health Check)" in {
      Get(s"/") ~> routes ~> check {
        status shouldEqual StatusCodes.OK
      }
    }
  }
}

