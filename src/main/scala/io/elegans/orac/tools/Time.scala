package io.elegans.orac.tools

import java.time.Instant

object Time {

  def getTimestampEpsoc: Long = {
    Instant.now.getEpochSecond
  }

  def getTimestampMillis: Long = {
    Instant.now().toEpochMilli
  }
}
