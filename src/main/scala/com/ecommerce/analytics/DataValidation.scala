package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset}

final case class ValidationResult[T](valid: Dataset[T], rejected: DataFrame)

final class DataValidation(config: AppConfig) {
  def validateTransactions(data: Dataset[Transaction]): ValidationResult[Transaction] =
    split(data, Seq(
      when(
        col("amount").isNull || col("amount") <= lit(config.transactionMinAmountExclusive),
        lit(s"amount must be > ${config.transactionMinAmountExclusive}")
      ),
      when(
        col("timestamp").isNull || length(col("timestamp")) =!= config.transactionTimestampLength,
        lit(s"timestamp must contain ${config.transactionTimestampLength} characters")
      )
    ))

  def validateUsers(data: Dataset[User]): ValidationResult[User] =
    split(data, Seq(
      when(
        col("age").isNull ||
          col("age") < lit(config.userMinAge) ||
          col("age") > lit(config.userMaxAge),
        lit(s"age must be between ${config.userMinAge} and ${config.userMaxAge}")
      ),
      when(
        col("annual_income").isNull ||
          col("annual_income") <= lit(config.userMinAnnualIncomeExclusive),
        lit(s"annual_income must be > ${config.userMinAnnualIncomeExclusive}")
      )
    ))

  def validateProducts(data: Dataset[Product]): ValidationResult[Product] =
    split(data, Seq(
      when(
        col("price").isNull || col("price") <= lit(config.productMinPriceExclusive),
        lit(s"price must be > ${config.productMinPriceExclusive}")
      ),
      when(
        col("rating").isNull ||
          col("rating") < lit(config.productMinRating) ||
          col("rating") > lit(config.productMaxRating),
        lit(s"rating must be between ${config.productMinRating} and ${config.productMaxRating}")
      )
    ))

  def validateMerchants(data: Dataset[Merchant]): ValidationResult[Merchant] =
    split(data, Seq(
      when(
        col("commission_rate").isNull ||
          col("commission_rate") < lit(config.merchantMinCommissionRate) ||
          col("commission_rate") > lit(config.merchantMaxCommissionRate),
        lit(
          s"commission_rate must be between ${config.merchantMinCommissionRate} " +
            s"and ${config.merchantMaxCommissionRate}"
        )
      )
    ))

  private def split[T](data: Dataset[T], violations: Seq[Column]): ValidationResult[T] = {
    val rejectionReason = concat_ws("; ", violations: _*)
    val evaluated = data.toDF().withColumn("rejection_reason", rejectionReason)
    val valid = evaluated
      .filter(col("rejection_reason") === "")
      .drop("rejection_reason")
      .as(data.encoder)
    val rejected = evaluated.filter(col("rejection_reason") =!= "")
    ValidationResult(valid, rejected)
  }
}
