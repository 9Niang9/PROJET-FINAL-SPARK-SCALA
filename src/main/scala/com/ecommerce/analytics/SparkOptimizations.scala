package com.ecommerce.analytics

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
 * Regroupe les optimisations Spark, activables depuis application.conf
 * (enable-cache, enable-broadcast) pour pouvoir comparer avec et sans.
 * Le nombre de partitions de shuffle est appliqué à la création de la session
 * (voir SparkSessionBuilder).
 */
final class SparkOptimizations(cacheEnabled: Boolean, broadcastEnabled: Boolean) {

  private val tracked = ListBuffer.empty[Dataset[_]]

  /** cache() : jeux de données réutilisés plusieurs fois et qui tiennent en mémoire. */
  def cache[T](data: Dataset[T]): Dataset[T] =
    if (cacheEnabled) { tracked += data; data.cache() } else data

  /** persist(MEMORY_AND_DISK_SER) : jeux volumineux, qui peuvent déborder sur disque. */
  def persist[T](data: Dataset[T]): Dataset[T] =
    if (cacheEnabled) { tracked += data; data.persist(StorageLevel.MEMORY_AND_DISK_SER) } else data

  /** broadcast() : petite table jointe à une grande, pour éviter un shuffle. */
  def broadcastIfEnabled[T](small: Dataset[T]): Dataset[T] =
    if (broadcastEnabled) broadcast(small) else small

  /** unpersist() : libère explicitement le cache quand il n'est plus utile. */
  def release(data: Dataset[_]): Unit = {
    data.unpersist()
    tracked -= data
  }

  def releaseAll(): Unit = {
    tracked.foreach(_.unpersist())
    tracked.clear()
  }
}
