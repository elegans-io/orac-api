package io.elegans.orac

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 15/03/17.
  */

import akka.actor.ActorSystem

object OracActorSystem {
  val system = ActorSystem("orac-service")
}
