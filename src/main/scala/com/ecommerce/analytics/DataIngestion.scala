package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types._

import scala.util.control.NonFatal

final class DataIngestion(spark: SparkSession, config: AppConfig) {
  import spark.implicits._

  def readTransactions(): Dataset[Transaction] =
    readDataset(config.transactionsPath) {
      spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(transactionSchema)
        .csv(config.transactionsPath)
        .as[Transaction]
    }

  def readUsers(): Dataset[User] =
    readDataset(config.usersPath) {
      spark.read
        .schema(userSchema)
        .option("mode", "PERMISSIVE")
        .json(config.usersPath)
        .as[User]
    }

  def readProducts(): Dataset[Product] =
    readDataset(config.productsPath) {
      spark.read.parquet(config.productsPath).as[Product]
    }

  def readMerchants(): Dataset[Merchant] =
    readDataset(config.merchantsPath) {
      spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config.merchantsPath)
        .withColumn("establishment_date", $"establishment_date".cast(StringType))
        .as[Merchant]
    }

  private def readDataset[T](path: String)(load: => Dataset[T]): Dataset[T] =
    try {
      val dataset = load
      val count = dataset.count()
      println(s"$path : $count lignes lues.")
      dataset
    } catch {
      case NonFatal(error) =>
        System.err.println(s"Échec de lecture de '$path' : ${error.getMessage}")
        throw new IllegalStateException(s"Impossible de lire le jeu de données '$path'.", error)
    }

  private val transactionSchema = StructType(Seq(
    StructField("transaction_id", StringType, nullable = true),
    StructField("user_id", StringType, nullable = true),
    StructField("product_id", StringType, nullable = true),
    StructField("merchant_id", StringType, nullable = true),
    StructField("amount", DoubleType, nullable = true),
    StructField("timestamp", StringType, nullable = true),
    StructField("location", StringType, nullable = true),
    StructField("payment_method", StringType, nullable = true),
    StructField("category", StringType, nullable = true)
  ))

  private val userSchema = StructType(Seq(
    StructField("user_id", StringType, nullable = true),
    StructField("age", IntegerType, nullable = true),
    StructField("annual_income", DoubleType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("customer_segment", StringType, nullable = true),
    StructField("preferred_categories", ArrayType(StringType), nullable = true),
    StructField("registration_date", StringType, nullable = true)
  ))
}
