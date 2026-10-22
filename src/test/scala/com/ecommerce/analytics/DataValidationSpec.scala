package com.ecommerce.analytics

import com.ecommerce.models.{Merchant, Product, Transaction, User}
import com.ecommerce.utils.AppConfig
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

final class DataValidationSpec extends AnyFunSuite with BeforeAndAfterAll {
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("data-validation-test")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override protected def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  test("rejects invalid transactions and preserves a rule-specific reason") {
    val config = AppConfig.load()
    val validation = new DataValidation(config)
    val session = spark
    import session.implicits._

    val transactionData = Seq(
      Transaction(
        "TX1", "U1", "P1", "M1", Some(1.0), "20260101123059", "Paris", "CARD", "Books"
      ),
      Transaction(
        "TX2", "U1", "P1", "M1", Some(0.0), "bad", "Paris", "CARD", "Books"
      )
    ).toDS()
    val transactionResult = validation.validateTransactions(transactionData)
    assert(transactionResult.valid.count() == 1L)
    assert(transactionResult.rejected.count() == 1L)
    assert(transactionResult.rejected.columns.exists(_ == "rejection_reason"))
    assert(
      transactionResult.rejected.first().getAs[String]("rejection_reason") ==
        "amount must be > 0.0; timestamp must contain 14 characters"
    )
  }

  test("rejects users outside the configured age and income bounds") {
    val validation = new DataValidation(AppConfig.load())
    val session = spark
    import session.implicits._

    val userData = Seq(
      User("U1", Some(16), Some(1.0), "Paris", "Standard", Seq("Books"), "20260101"),
      User("U2", Some(15), Some(0.0), "Paris", "Standard", Seq("Books"), "20260101")
    ).toDS()
    val userResult = validation.validateUsers(userData)
    assert(userResult.valid.count() == 1L)
    assert(userResult.rejected.count() == 1L)
    assert(userResult.rejected.first().getAs[String]("rejection_reason") ==
      "age must be between 16 and 100; annual_income must be > 0.0")
  }

  test("rejects products outside the configured price and rating bounds") {
    val validation = new DataValidation(AppConfig.load())
    val session = spark
    import session.implicits._

    val productData = Seq(
      Product("P1", "Book", "Books", Some(1.0), "M1", Some(5.0), Some(1)),
      Product("P2", "Book", "Books", Some(0.0), "M1", Some(0.0), Some(1))
    ).toDS()
    val productResult = validation.validateProducts(productData)
    assert(productResult.valid.count() == 1L)
    assert(productResult.rejected.count() == 1L)
    assert(productResult.rejected.first().getAs[String]("rejection_reason") ==
      "price must be > 0.0; rating must be between 1.0 and 5.0")
  }

  test("rejects merchant commission rates outside the inclusive bounds") {
    val validation = new DataValidation(AppConfig.load())
    val session = spark
    import session.implicits._

    val merchantData = Seq(
      Merchant("M1", "Shop", "Books", "Paris", Some(0.0), "20200101"),
      Merchant("M2", "Shop", "Books", "Paris", Some(1.1), "20200101")
    ).toDS()
    val merchantResult = validation.validateMerchants(merchantData)
    assert(merchantResult.valid.count() == 1L)
    assert(merchantResult.rejected.count() == 1L)
    assert(merchantResult.rejected.first().getAs[String]("rejection_reason") ==
      "commission_rate must be between 0.0 and 1.0")
  }
}
