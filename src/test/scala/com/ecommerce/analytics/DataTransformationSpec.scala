package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, to_timestamp}
import org.scalatest.funsuite.AnyFunSuite

class DataTransformationSpec extends AnyFunSuite {

  private lazy val spark = SparkSession.builder()
    .master("local[1]")
    .appName("tests-transformation")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  test("fenetres : cumul 7 jours, utilisateur actif et delai depuis l'achat precedent") {
    import spark.implicits._

    val df = Seq(
      ("T1", "U1", 100.0, "2024-01-01 10:00:00"),
      ("T2", "U1",  50.0, "2024-01-02 10:00:00"),
      ("T3", "U1",  10.0, "2024-01-03 10:00:00"),
      ("T4", "U1",  10.0, "2024-01-04 10:00:00"),
      ("T5", "U1",  10.0, "2024-01-05 10:00:00"),
      ("T6", "U1",   5.0, "2024-01-20 10:00:00")
    ).toDF("transaction_id", "user_id", "amount", "ts")
      .withColumn("transaction_ts", to_timestamp(col("ts")))
      .withColumn("transaction_date", to_date(col("transaction_ts")))

    val rows = new DataTransformation(spark, AppConfig.load())
      .addBehaviorFeatures(df)
      .orderBy("transaction_id")
      .collect()

    def amount7d(i: Int) = rows(i).getAs[Double]("amount_7d")
    def active(i: Int)   = rows(i).getAs[Int]("is_active_user")

    assert(amount7d(0) == 100.0)
    assert(amount7d(4) == 180.0)   // 100 + 50 + 10 + 10 + 10
    assert(amount7d(5) == 5.0)     // les achats de janvier sont sortis de la fenetre
    assert(active(3) == 0)         // 4 jours distincts seulement
    assert(active(4) == 1)         // 5 jours distincts
    assert(active(5) == 0)

    assert(rows(0).isNullAt(rows(0).fieldIndex("days_since_previous_tx")))
    assert(rows(1).getAs[Int]("days_since_previous_tx") == 1)
    assert(rows(1).getAs[Long]("seconds_since_previous_tx") == 86400L)
    assert(rows(5).getAs[Int]("days_since_previous_tx") == 15)
  }

  test("transactions suspectes : au moins 2 conditions sur 4") {
    import spark.implicits._

    val small = (1 to 8).map(i => (s"S$i", "U1", 10.0, "Morning", Option(100000L), "CARD"))
    val data = (small ++ Seq(
      ("BIG",  "U1", 1000.0, "Afternoon", Option(100000L),   "CRYPTO"), // montant eleve + crypto = 2
      ("NITE", "U2",   50.0, "Night",     Option(60L),        "CRYPTO"), // nuit + rapide + crypto = 3
      ("FRST", "U3",   50.0, "Night",     Option.empty[Long], "CARD"),   // nuit seulement = 1
      ("FAST", "U4",   50.0, "Morning",   Option(200L),       "CRYPTO")  // rapide + crypto = 2
    )).toDF("transaction_id", "user_id", "amount", "day_period", "seconds_since_previous_tx", "payment_method")

    val result = new DataTransformation(spark, AppConfig.load())
      .addSuspiciousFlags(data)
      .collect()
      .map(r => r.getAs[String]("transaction_id") -> ((r.getAs[Int]("is_suspicious"), r.getAs[Int]("suspicion_score"))))
      .toMap

    assert(result("BIG")  == ((1, 2)))
    assert(result("NITE") == ((1, 3)))
    assert(result("FRST") == ((0, 1)))   // un delai inconnu (premier achat) ne declenche pas la regle "rapide"
    assert(result("FAST") == ((1, 2)))
    (1 to 8).foreach(i => assert(result(s"S$i") == ((0, 0))))
    assert(result.values.count(_._1 == 1) == 3)
  }
}
