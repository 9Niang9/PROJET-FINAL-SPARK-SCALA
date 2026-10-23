package com.ecommerce.utils

import org.apache.spark.sql.DataFrame

/** Écrit chaque résultat final aux formats CSV et Parquet dans le répertoire de sortie. */
final class ResultsWriter(outputDir: String) {
  private val base = outputDir.stripSuffix("/")

  def write(name: String, result: DataFrame): Unit = {
    // Les résultats agrégés sont petits : un seul fichier par résultat suffit.
    val single = result.coalesce(1)
    single.write.mode("overwrite").option("header", "true").csv(s"$base/csv/$name")
    single.write.mode("overwrite").parquet(s"$base/parquet/$name")
    println(s"[ECRITURE] $name -> $base/csv/$name et $base/parquet/$name")
  }
}
