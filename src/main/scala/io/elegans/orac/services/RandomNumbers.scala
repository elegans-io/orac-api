package io.elegans.orac.services

object RandomNumbers {
  val random: scala.util.Random.type = scala.util.Random

  def intPos: Int = {
    math.abs(random.nextInt)
  }

  def floatPos: Float = {
    math.abs(random.nextFloat)
  }

  def doublePos: Double = {
    math.abs(random.nextDouble)
  }

  def int: Int = {
    random.nextInt
  }

  def float: Float = {
    random.nextFloat
  }

  def double: Double = {
    random.nextDouble
  }

  def long: Long = {
    random.nextLong()
  }

  def string(size: Int): String = {
    random.alphanumeric.take(size).mkString
  }
}
