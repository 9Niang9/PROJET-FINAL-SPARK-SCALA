package com.ecommerce.analytics

import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, lit, sum}

/** Exécution de la chaîne ingestion, validation et transformation avec contrôles. */
object TransformationApp {

  def main(args: Array[String]): Unit = {
    val cfg   = AppConfig.load()
    val spark = SparkSession.builder()
      .appName(cfg.appName)
      .master(cfg.sparkMaster)
      .config("spark.sql.shuffle.partitions", cfg.shufflePartitions.toString)
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      val ingestion  = new DataIngestion(spark, cfg)
      val validation = new DataValidation(cfg)

      val users        = ingestion.readUsers()
      val products     = ingestion.readProducts()
      val merchants    = ingestion.readMerchants()
      val validTx      = validation.validateTransactions(ingestion.readTransactions()).valid

      val transformation = new DataTransformation(spark, cfg, cfg.broadcastEnabled)
      val enriched = transformation
        .addSuspiciousFlags(
          transformation.addBehaviorFeatures(transformation.enrichTransactionData(validTx, users, products, merchants))
        )
        .cache()

      println(s"[TRANSFORMATION] lignes enrichies : ${enriched.count()} (attendu : celles des transactions valides)")
      enriched.printSchema()

      enriched.select(
        "transaction_id", "user_id", "amount", "transaction_ts", "hour", "day_of_week", "month",
        "is_weekend", "day_period", "is_working_hours", "age_group",
        "user_tx_rank", "user_tx_count", "amount_7d", "active_days_7d", "is_active_user",
        "days_since_previous_tx"
      ).orderBy("user_id", "user_tx_rank").show(12, truncate = false)

      // Contrôles
      println("[CONTROLE] references orphelines (colonnes jointes nulles) :")
      println(s"  client inconnu   : ${enriched.filter(col("customer_segment").isNull).count()}")
      println(s"  produit inconnu  : ${enriched.filter(col("product_category").isNull).count()}")
      println(s"  marchand inconnu : ${enriched.filter(col("merchant_name").isNull).count()}")
      println(s"[CONTROLE] timestamps non decodes par l'UDF : ${enriched.filter(col("day_period").isNull).count()}")
      println(s"[CONTROLE] transactions d'utilisateurs actifs : ${enriched.filter(col("is_active_user") === 1).count()}")
      println(s"[CONTROLE] premieres transactions (pas de precedente) : ${enriched.filter(col("days_since_previous_tx").isNull).count()}")
      println("[CONTROLE] repartition par tranche d'age :")
      enriched.groupBy("age_group").agg(count(lit(1)).as("nb_transactions")).orderBy("age_group").show(false)
      println("[CONTROLE] repartition par periode de la journee :")
      enriched.groupBy("day_period").agg(count(lit(1)).as("nb_transactions")).orderBy("day_period").show(false)

      println(s"[CONTROLE] transactions suspectes : ${enriched.filter(col("is_suspicious") === 1).count()}")
      enriched.unpersist()
    } catch {
      case e: Exception => println(s"[ERREUR FATALE] ${e.getMessage}")
    } finally {
      spark.stop()
    }
  }
}
