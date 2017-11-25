package io.elegans.orac.tools

import java.time.Instant

object Time {

  def getTimestampEpoc(): Long = {
    Instant.now.getEpochSecond
  }
}
