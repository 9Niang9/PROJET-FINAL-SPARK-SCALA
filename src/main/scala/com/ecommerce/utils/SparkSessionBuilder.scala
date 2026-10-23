package com.ecommerce.utils

import org.apache.spark.sql.SparkSession

/** Construit la SparkSession à partir de application.conf (aucun paramètre codé en dur). */
object SparkSessionBuilder {

  def build(cfg: AppConfig): SparkSession = {
    val builder = SparkSession.builder()
      .appName(cfg.appName)
      .config("spark.sql.shuffle.partitions", cfg.shufflePartitions.toString)
      .config("spark.sql.session.timeZone", "UTC")

    // Avec spark-submit, l'option --master fournit déjà spark.master : on ne
    // l'écrase pas avec la valeur du fichier de configuration (utile en cluster).
    val configured =
      if (sys.props.contains("spark.master")) builder else builder.master(cfg.sparkMaster)

    val spark = configured.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
