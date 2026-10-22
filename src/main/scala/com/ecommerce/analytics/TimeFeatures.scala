package com.ecommerce.analytics

import java.time.{DayOfWeek, LocalDateTime}
import java.time.format.{DateTimeFormatter, ResolverStyle}

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.util.Try

/** Caractéristiques temporelles extraites d'un timestamp au format yyyyMMddHHmmss. */
final case class TimeFeatures(
  hour: Int,
  day_of_week: String,
  month: String,
  is_weekend: Int,
  day_period: String,
  is_working_hours: Int
)

object TimeFeatures {

  // Résolution stricte : les dates impossibles sont rejetées
  private val formatter: DateTimeFormatter =
    DateTimeFormatter.ofPattern("uuuuMMddHHmmss").withResolverStyle(ResolverStyle.STRICT)

  private val joursFr: Map[DayOfWeek, String] = Map(
    DayOfWeek.MONDAY -> "Lundi", DayOfWeek.TUESDAY -> "Mardi", DayOfWeek.WEDNESDAY -> "Mercredi",
    DayOfWeek.THURSDAY -> "Jeudi", DayOfWeek.FRIDAY -> "Vendredi",
    DayOfWeek.SATURDAY -> "Samedi", DayOfWeek.SUNDAY -> "Dimanche"
  )

  private val moisFr: Vector[String] = Vector(
    "Janvier", "Février", "Mars", "Avril", "Mai", "Juin",
    "Juillet", "Août", "Septembre", "Octobre", "Novembre", "Décembre"
  )

  /** Morning 6h-12h, Afternoon 12h-18h, Evening 18h-22h, Night sinon. */
  def dayPeriod(hour: Int): String =
    if (hour >= 6 && hour < 12) "Morning"
    else if (hour >= 12 && hour < 18) "Afternoon"
    else if (hour >= 18 && hour < 22) "Evening"
    else "Night"

  /** Renvoie None si le timestamp est nul ou invalide. */
  def extract(ts: String): Option[TimeFeatures] =
    Option(ts)
      .map(_.trim)
      .filter(_.matches("\\d{14}"))
      .flatMap(s => Try(LocalDateTime.parse(s, formatter)).toOption)
      .map { dt =>
        val h   = dt.getHour
        val dow = dt.getDayOfWeek
        TimeFeatures(
          hour             = h,
          day_of_week      = joursFr(dow),
          month            = moisFr(dt.getMonthValue - 1),
          is_weekend       = if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) 1 else 0,
          day_period       = dayPeriod(h),
          is_working_hours = if (h >= 9 && h <= 17) 1 else 0
        )
      }

  /** UDF Spark retournant un struct (null si le timestamp est invalide). */
  val extractTimeFeatures: UserDefinedFunction = udf((ts: String) => extract(ts))
}
