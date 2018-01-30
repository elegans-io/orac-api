package io.elegans.orac.routing

import akka.pattern.CircuitBreaker
import io.elegans.orac.OracActorSystem

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object OracCircuitBreaker {
  def getCircuitBreaker(maxFailure: Int = 10, callTimeout: FiniteDuration = 10.seconds,
                        resetTimeout: FiniteDuration = 2.seconds): CircuitBreaker = {
    val breaker = new CircuitBreaker(scheduler = OracActorSystem.system.scheduler,
      maxFailures = maxFailure,
      callTimeout = callTimeout,
      resetTimeout = resetTimeout
    )
    breaker
  }
}


