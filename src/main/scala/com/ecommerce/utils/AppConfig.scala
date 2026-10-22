package com.ecommerce.utils

import com.typesafe.config.{Config, ConfigFactory}

final class AppConfig private (private val config: Config) {
  def appName: String = config.getString("app.name")
  def sparkMaster: String = config.getString("app.spark.master")
  def shufflePartitions: Int = config.getInt("app.spark.shuffle.partitions")
  def cacheEnabled: Boolean = config.getBoolean("app.optimization.enable-cache")
  def broadcastEnabled: Boolean = config.getBoolean("app.optimization.enable-broadcast")

  def transactionsPath: String = config.getString("app.data.input.transactions")
  def usersPath: String = config.getString("app.data.input.users")
  def productsPath: String = config.getString("app.data.input.products")
  def merchantsPath: String = config.getString("app.data.input.merchants")
  def qualityReportPath: String = config.getString("app.data.output.quality-report")

  def transactionMinAmountExclusive: Double =
    config.getDouble("app.validation.transaction.min-amount-exclusive")
  def transactionTimestampLength: Int =
    config.getInt("app.validation.transaction.timestamp-length")
  def userMinAge: Int = config.getInt("app.validation.user.min-age")
  def userMaxAge: Int = config.getInt("app.validation.user.max-age")
  def userMinAnnualIncomeExclusive: Double =
    config.getDouble("app.validation.user.min-annual-income-exclusive")
  def productMinPriceExclusive: Double =
    config.getDouble("app.validation.product.min-price-exclusive")
  def productMinRating: Double = config.getDouble("app.validation.product.min-rating")
  def productMaxRating: Double = config.getDouble("app.validation.product.max-rating")
  def merchantMinCommissionRate: Double =
    config.getDouble("app.validation.merchant.min-commission-rate")
  def merchantMaxCommissionRate: Double =
    config.getDouble("app.validation.merchant.max-commission-rate")
}

object AppConfig {
  private val defaults = ConfigFactory.parseString("""
    app {
      name = "EcommerceAnalytics"
      spark { master = "local[*]", shuffle.partitions = 8 }
      optimization { enable-cache = true, enable-broadcast = true }
      data {
        input {
          transactions = "data/transactions.csv"
          users = "data/users.json"
          products = "data/products.parquet"
          merchants = "data/merchants.csv"
        }
        output { quality-report = "output/data_quality_report" }
      }
      validation {
        transaction { min-amount-exclusive = 0.0, timestamp-length = 14 }
        user {
          min-age = 16, max-age = 100
          min-annual-income-exclusive = 0.0
        }
        product {
          min-price-exclusive = 0.0, min-rating = 1.0, max-rating = 5.0
        }
        merchant { min-commission-rate = 0.0, max-commission-rate = 1.0 }
      }
    }
  """)

  def load(): AppConfig =
    new AppConfig(ConfigFactory.load().withFallback(defaults).resolve())
}
