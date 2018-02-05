package io.elegans.orac.tools

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17
  */

import java.time.Instant

object Time {

  def timestampEpoc: Long = {
    Instant.now.getEpochSecond
  }

  def timestampMillis: Long = {
    Instant.now().toEpochMilli
  }
}
