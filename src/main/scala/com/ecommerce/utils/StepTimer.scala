package com.ecommerce.utils

import java.time.Instant

import scala.collection.mutable.ListBuffer

/** Mesure et journalise heure de début, heure de fin et durée de chaque étape du pipeline. */
final class StepTimer {
  private val durations = ListBuffer.empty[(String, Long)]

  def time[T](step: String)(body: => T): T = {
    val start = Instant.now()
    println(s"[ETAPE] $step : début à $start")
    val t0 = System.nanoTime()
    try body
    finally {
      val millis = (System.nanoTime() - t0) / 1000000L
      durations += ((step, millis))
      println(s"[ETAPE] $step : fin à ${Instant.now()} (durée : $millis ms)")
    }
  }

  def printSummary(label: String): Unit = {
    if (durations.nonEmpty) {
      println(s"[DUREES] $label")
      durations.foreach { case (step, ms) => println(f"  $step%-15s $ms%8d ms") }
      println(f"  ${"total"}%-15s ${durations.map(_._2).sum}%8d ms")
    }
  }
}
