package io.elegans.orac.routing

import akka.event.LoggingAdapter
import akka.http.scaladsl.model.StatusCodes
import akka.pattern.CircuitBreaker
import io.elegans.orac.OracActorSystem

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import scala.concurrent.ExecutionContext.Implicits.global

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


