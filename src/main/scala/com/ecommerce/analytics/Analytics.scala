package com.ecommerce.analytics

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

/** Résultats de l'analyse de cohortes (Question 4.2). */
final case class CohortAnalysis(
    retention: DataFrame,
    revenue: DataFrame,
    bestAtThreeMonths: DataFrame
)

/** Analytique business : KPI par marchand et cohortes. Travaille sur le DataFrame enrichi (Partie 3). */
final class Analytics(optimizations: SparkOptimizations) {

  private val ageGroups = Seq(
    "Jeune" -> "sales_jeune",
    "Adulte" -> "sales_adulte",
    "Âge Moyen" -> "sales_age_moyen",
    "Senior" -> "sales_senior",
    "Inconnu" -> "sales_inconnu"
  )

  // ---------------------------------------------------------------------------
  // Question 4.1 : rapport détaillé par marchand
  // ---------------------------------------------------------------------------

  /**
   * Une ligne par marchand : chiffre d'affaires, volume, clients uniques, panier moyen,
   * commission, classements (catégorie et région), ventes par tranche d'âge et taux de
   * transactions suspectes (bonus). Les transactions dont le marchand est inconnu du
   * référentiel sont exclues : on ne peut ni les nommer ni les classer.
   */
  def merchantReport(enriched: DataFrame): DataFrame = {
    val known = enriched.filter(col("merchant_name").isNotNull)

    val kpis = known
      .groupBy("merchant_id", "merchant_name", "merchant_category", "region")
      .agg(
        round(sum("amount"), 2).as("total_revenue"),
        count(lit(1)).as("nb_transactions"),
        countDistinct("user_id").as("nb_unique_customers"),
        round(avg("amount"), 2).as("avg_transaction_amount"),
        // une commission hors bornes a été remplacée par null en Partie 3 : elle est ignorée
        round(sum(col("amount") * col("commission_rate")), 2).as("total_commission"),
        round(avg(col("is_suspicious")) * 100, 2).as("suspicious_rate_pct")
      )

    val byCategory = Window.partitionBy("merchant_category").orderBy(col("total_revenue").desc)
    val byRegion = Window.partitionBy("region").orderBy(col("total_revenue").desc)

    kpis
      .withColumn("rank_in_category", rank().over(byCategory))
      .withColumn("rank_in_region", rank().over(byRegion))
      .join(optimizations.broadcastIfEnabled(salesByAgeGroup(known)), Seq("merchant_id"), "left")
      .orderBy(col("total_revenue").desc)
  }

  /** Répartition du chiffre d'affaires de chaque marchand par tranche d'âge (une colonne par tranche). */
  def salesByAgeGroup(data: DataFrame): DataFrame = {
    val pivoted = data
      .groupBy("merchant_id")
      .pivot("age_group", ageGroups.map(_._1))
      .agg(round(sum("amount"), 2))

    val renamed = ageGroups.foldLeft(pivoted) { case (df, (group, name)) =>
      df.withColumnRenamed(group, name)
    }
    renamed.na.fill(0.0, ageGroups.map(_._2))
  }

  // ---------------------------------------------------------------------------
  // Question 4.2 : analyse de cohortes
  // ---------------------------------------------------------------------------

  def cohortAnalysis(enriched: DataFrame): CohortAnalysis = {
    val transactions = enriched
      .filter(col("user_id").isNotNull && col("transaction_date").isNotNull)
      .select(
        col("user_id"),
        col("amount"),
        trunc(col("transaction_date"), "month").as("tx_month")
      )

    // cohort_month : mois de la première transaction de l'utilisateur
    val cohorts = transactions
      .groupBy("user_id")
      .agg(min("tx_month").as("cohort_month"))

    val cohortSizes = cohorts
      .groupBy("cohort_month")
      .agg(count(lit(1)).as("cohort_size"))

    // period_index : nombre de mois entre la transaction et le mois de la cohorte
    val activity = transactions
      .join(cohorts, Seq("user_id"))
      .withColumn("period_index", months_between(col("tx_month"), col("cohort_month")).cast("int"))

    val sizes = optimizations.broadcastIfEnabled(cohortSizes)

    val retention = activity
      .groupBy("cohort_month", "period_index")
      .agg(countDistinct("user_id").as("active_users"))
      .join(sizes, Seq("cohort_month"))
      .withColumn("retention_rate_pct", round(col("active_users") / col("cohort_size") * 100, 2))
      .orderBy("cohort_month", "period_index")

    // Bonus : chiffre d'affaires par cohorte et par période, revenu moyen par utilisateur actif
    val revenue = activity
      .groupBy("cohort_month", "period_index")
      .agg(
        round(sum("amount"), 2).as("revenue"),
        countDistinct("user_id").as("active_users")
      )
      .withColumn("avg_revenue_per_user", round(col("revenue") / col("active_users"), 2))
      .orderBy("cohort_month", "period_index")

    val bestAtThreeMonths = retention
      .filter(col("period_index") === 3)
      .orderBy(col("retention_rate_pct").desc, col("cohort_size").desc)
      .limit(1)

    CohortAnalysis(
      format(retention),
      format(revenue),
      format(bestAtThreeMonths)
    )
  }

  /** Affiche cohort_month sous la forme yyyy-MM. */
  private def format(df: DataFrame): DataFrame =
    df.withColumn("cohort_month", date_format(col("cohort_month"), "yyyy-MM"))
}
