package com.ecommerce.analytics

import com.ecommerce.utils.{AppConfig, ResultsWriter, SparkSessionBuilder, StepTimer}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.NonFatal

/**
 * Point d'entrée de l'application (Partie 6).
 *
 * Étapes acceptées en argument (Question 6.2, bonus) :
 *   ingestion       : lecture, validation et rapport de qualité
 *   transformation  : ingestion + enrichissement des transactions
 *   analytics       : transformation + KPI marchands et cohortes + écriture des résultats
 *   all             : identique à analytics (valeur par défaut)
 */
object MainApp {

  private val Stages = Seq("ingestion", "transformation", "analytics", "all")

  def main(args: Array[String]): Unit = {
    val stage = args.headOption.map(_.trim.toLowerCase).getOrElse("all")

    if (!Stages.contains(stage)) {
      println(s"Étape inconnue : '${args.head}'.")
      println(s"Valeurs acceptées : ${Stages.mkString(", ")} (par défaut : all).")
      return
    }

    val cfg = AppConfig.load()
    val spark = SparkSessionBuilder.build(cfg)
    val timer = new StepTimer
    val optimizations = new SparkOptimizations(cfg.cacheEnabled, cfg.broadcastEnabled)

    println(
      s"[CONFIG] étape=$stage, cache=${cfg.cacheEnabled}, broadcast=${cfg.broadcastEnabled}, " +
        s"shuffle.partitions=${cfg.shufflePartitions}"
    )

    var failed = false
    try {
      run(stage, spark, cfg, timer, optimizations)
    } catch {
      case NonFatal(error) =>
        failed = true
        System.err.println(s"[ERREUR FATALE] ${error.getMessage}")
    } finally {
      // Arrêt propre quoi qu'il arrive : libération du cache puis de la SparkSession.
      optimizations.releaseAll()
      spark.stop()
    }

    timer.printSummary(s"cache=${cfg.cacheEnabled}, broadcast=${cfg.broadcastEnabled}")
    if (failed) sys.exit(1)
  }

  private def run(
      stage: String,
      spark: SparkSession,
      cfg: AppConfig,
      timer: StepTimer,
      optimizations: SparkOptimizations
  ): Unit = {
    val ingestion = new DataIngestion(spark, cfg)
    val validation = new DataValidation(cfg)

    // 1. Ingestion : les transactions sont lues une seule fois puis réutilisées -> cache()
    val (transactions, users, products, merchants) = timer.time("ingestion") {
      (
        optimizations.cache(ingestion.readTransactions()),
        optimizations.cache(ingestion.readUsers()),
        optimizations.cache(ingestion.readProducts()),
        optimizations.cache(ingestion.readMerchants())
      )
    }

    // 2. Validation et rapport de qualité (Partie 2)
    val validTransactions = timer.time("validation") {
      val vTransactions = validation.validateTransactions(transactions)
      val vUsers = validation.validateUsers(users)
      val vProducts = validation.validateProducts(products)
      val vMerchants = validation.validateMerchants(merchants)

      new DataQualityReport(spark, cfg).createAndSave(Seq(
        ("transactions", transactions.toDF(), vTransactions.valid.toDF()),
        ("users", users.toDF(), vUsers.valid.toDF()),
        ("products", products.toDF(), vProducts.valid.toDF()),
        ("merchants", merchants.toDF(), vMerchants.valid.toDF())
      ))
      vTransactions.valid
    }

    if (stage == "ingestion") return

    // 3. Transformation (Partie 3) : résultat volumineux et large -> persist sur mémoire et disque
    val enriched = timer.time("transformation") {
      val transformation = new DataTransformation(spark, cfg, cfg.broadcastEnabled)
      val result = transformation.addSuspiciousFlags(
        transformation.addBehaviorFeatures(
          transformation.enrichTransactionData(validTransactions, users, products, merchants)
        )
      )
      val persisted = optimizations.persist(result)
      println(s"[TRANSFORMATION] lignes enrichies : ${persisted.count()}")
      persisted
    }

    if (stage == "transformation") {
      enriched.select(
        "transaction_id", "user_id", "amount", "day_period", "age_group",
        "user_tx_rank", "amount_7d", "is_active_user", "is_suspicious"
      ).show(10, truncate = false)
      return
    }

    // 4. Analytique (Partie 4)
    val analytics = new Analytics(optimizations)
    val (merchantReport, cohorts) = timer.time("analytique") {
      val report = analytics.merchantReport(enriched)
      val cohortResults = analytics.cohortAnalysis(enriched)

      println("[RESULTAT] Top 10 des marchands par chiffre d'affaires :")
      report.show(10, truncate = false)
      println("[RESULTAT] Matrice de rétention des cohortes (extrait) :")
      cohortResults.retention.show(15, truncate = false)
      println("[RESULTAT] Cohorte avec la meilleure rétention à 3 mois :")
      cohortResults.bestAtThreeMonths.show(truncate = false)
      (report, cohortResults)
    }

    // 5. Écriture des résultats en CSV et Parquet
    timer.time("ecriture") {
      val writer = new ResultsWriter(cfg.outputPath)
      val results: Seq[(String, DataFrame)] = Seq(
        "merchant_report" -> merchantReport,
        "cohort_retention" -> cohorts.retention,
        "cohort_revenue" -> cohorts.revenue,
        "cohort_best_3_months" -> cohorts.bestAtThreeMonths
      )
      results.foreach { case (name, df) => writer.write(name, df) }
    }
  }
}
