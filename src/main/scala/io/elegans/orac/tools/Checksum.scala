package io.elegans.orac.tools

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 10/11/17
  */

import java.security.MessageDigest

object Checksum {
  def sha512(source: String): String = {
    val checksum: String = MessageDigest.getInstance("SHA-512")
      .digest(source.getBytes).map(0xFF & _).map { "%02x".format(_) }.foldLeft(""){_ + _}
    checksum
  }
}
