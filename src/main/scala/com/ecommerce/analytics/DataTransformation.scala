package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/** Enrichissement des transactions : jointures, variables temporelles, fenêtres et détection de suspicion. */
class DataTransformation(spark: SparkSession, cfg: AppConfig, enableBroadcast: Boolean = true) {

  private val timeFields = Seq("hour", "day_of_week", "month", "is_weekend", "day_period", "is_working_hours")

  /** Joint transactions, clients, produits et marchands (left joins) et ajoute les variables temporelles. */
  def enrichTransactionData(
      transactions: Dataset[Transaction],
      users: Dataset[User],
      products: Dataset[Product],
      merchants: Dataset[Merchant]
  ): DataFrame = {

    // Horodatage et variables temporelles
    val withTs = transactions.toDF()
      .withColumn("transaction_ts", to_timestamp(col("timestamp"), "yyyyMMddHHmmss"))
      .withColumn("transaction_date", to_date(col("transaction_ts")))
      .withColumn("time_features", TimeFeatures.extractTimeFeatures(col("timestamp")))

    val withTime = timeFields
      .foldLeft(withTs)((df, f) => df.withColumn(f, col(s"time_features.$f")))
      .drop("time_features")

    // Tables de référence : colonnes renommées, valeurs hors bornes remplacées par null
    val u = users.toDF().select(
      col("user_id"),
      when(col("age").between(cfg.userMinAge, cfg.userMaxAge), col("age")).as("age"),
      when(col("annual_income") > cfg.userMinAnnualIncomeExclusive, col("annual_income")).as("annual_income"),
      col("city").as("user_city"),
      col("customer_segment"),
      col("preferred_categories"),
      col("registration_date")
    )

    val p = products.toDF().select(
      col("product_id"),
      col("name").as("product_name"),
      col("category").as("product_category"),
      when(col("price") > cfg.productMinPriceExclusive, col("price")).as("price"),
      when(col("rating").between(cfg.productMinRating, cfg.productMaxRating), col("rating")).as("rating"),
      col("stock")
    )

    val m = merchants.toDF().select(
      col("merchant_id"),
      col("name").as("merchant_name"),
      col("category").as("merchant_category"),
      col("region"),
      when(col("commission_rate").between(cfg.merchantMinCommissionRate, cfg.merchantMaxCommissionRate), col("commission_rate"))
        .as("commission_rate"),
      col("establishment_date")
    )
    val mJoin = if (enableBroadcast) broadcast(m) else m

    val joined = withTime
      .join(u, Seq("user_id"), "left")
      .join(p, Seq("product_id"), "left")
      .join(mJoin, Seq("merchant_id"), "left")

    // Tranche d'âge (25 ans classé Adulte)
    val withAgeGroup = joined.withColumn(
      "age_group",
      when(col("age").isNull, "Inconnu")
        .when(col("age") < 25, "Jeune")
        .when(col("age") < 45, "Adulte")
        .when(col("age") < 65, "Âge Moyen")
        .otherwise("Senior")
    )

    val perUserOrdered = Window.partitionBy("user_id").orderBy(col("transaction_ts"), col("transaction_id"))
    val perUser        = Window.partitionBy("user_id")

    withAgeGroup
      .withColumn("user_tx_rank", row_number().over(perUserOrdered))
      .withColumn("user_tx_count", count(lit(1)).over(perUser))
  }

  /** Montant sur 7 jours glissants, jours actifs et délai depuis la transaction précédente. */
  def addBehaviorFeatures(df: DataFrame): DataFrame = {
    val sevenDaysInSeconds = 7L * 24 * 3600

    // Fenêtre de 7 jours glissants en secondes
    val last7Days = Window
      .partitionBy("user_id")
      .orderBy(col("transaction_ts").cast("long"))
      .rangeBetween(-sevenDaysInSeconds, 0)

    // Fenêtre en jours calendaires (jour courant et 6 précédents)
    val last7Calendar = Window
      .partitionBy("user_id")
      .orderBy(datediff(col("transaction_date"), lit("1970-01-01")))
      .rangeBetween(-6, 0)

    val ordered = Window.partitionBy("user_id").orderBy(col("transaction_ts"), col("transaction_id"))

    df
      .withColumn("amount_7d", sum(col("amount")).over(last7Days))
      // countDistinct n'est pas supporté en fenêtre : collect_set puis size
      .withColumn("active_days_7d", size(collect_set(col("transaction_date")).over(last7Calendar)))
      .withColumn("is_active_user", when(col("active_days_7d") >= 5, 1).otherwise(0))
      .withColumn("previous_tx_ts", lag(col("transaction_ts"), 1).over(ordered))
      .withColumn("days_since_previous_tx", datediff(col("transaction_date"), to_date(col("previous_tx_ts"))))
      .withColumn("seconds_since_previous_tx",
        col("transaction_ts").cast("long") - col("previous_tx_ts").cast("long"))
      .drop("previous_tx_ts")
  }

  /**
   * Marque une transaction comme suspecte si au moins 2 critères sont remplis :
   * montant supérieur de plus de 300 % à la moyenne du client, transaction de nuit,
   * moins de 5 minutes après la précédente, paiement en CRYPTO.
   */
  def addSuspiciousFlags(df: DataFrame): DataFrame = {
    val perUser = Window.partitionBy("user_id")

    df
      .withColumn("user_avg_amount", avg(col("amount")).over(perUser))
      .withColumn("amount_deviation_pct",
        (col("amount") - col("user_avg_amount")) / col("user_avg_amount") * 100)
      .withColumn("susp_high_amount", when(col("amount_deviation_pct") > 300, 1).otherwise(0))
      .withColumn("susp_night",       when(col("day_period") === "Night", 1).otherwise(0))
      .withColumn("susp_rapid",       when(col("seconds_since_previous_tx") < 300, 1).otherwise(0))
      .withColumn("susp_crypto",      when(col("payment_method") === "CRYPTO", 1).otherwise(0))
      .withColumn("suspicion_score",
        col("susp_high_amount") + col("susp_night") + col("susp_rapid") + col("susp_crypto"))
      .withColumn("is_suspicious", when(col("suspicion_score") >= 2, 1).otherwise(0))
      // Arrondis après le calcul des critères
      .withColumn("user_avg_amount", round(col("user_avg_amount"), 2))
      .withColumn("amount_deviation_pct", round(col("amount_deviation_pct"), 2))
  }
}
