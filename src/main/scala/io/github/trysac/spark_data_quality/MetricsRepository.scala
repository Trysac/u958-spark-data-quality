package io.github.trysac.spark_data_quality

import scala.collection.mutable.ArrayBuffer

/** Timestamped identifier and metadata tags for a verification run.
  *
  * @param epochMillis
  *   timestamp of the run in milliseconds since the Unix epoch
  * @param tags
  *   arbitrary key-value labels describing the dataset or environment
  */
final case class ResultKey(epochMillis: Long, tags: Map[String, String] = Map.empty)

/** Storage for historical verification metrics used by anomaly detection. */
trait MetricsRepository {

  /** Saves `metrics` under `key`.
    *
    * If `key` already exists, its metrics are updated in place.
    */
  def save(key: ResultKey, metrics: Map[String, Double]): Unit

  /** Returns all historical values for `metric` with `epochMillis < before`, ordered by ascending
    * `epochMillis`.
    */
  def history(metric: String, before: Long): Vector[(ResultKey, Double)]
}

/** Bounded in-memory implementation of [[MetricsRepository]].
  *
  * @param maxEntries
  *   maximum number of distinct [[ResultKey]] entries stored (default 10_000)
  */
final class InMemoryMetricsRepository(val maxEntries: Int = 10_000) extends MetricsRepository {

  if (maxEntries < 1) {
    throw DataQualityException(
      "invalid_repository_capacity",
      s"maxEntries must be >= 1, got $maxEntries",
    )
  }

  private val entries = ArrayBuffer.empty[(ResultKey, Map[String, Double])]

  /** Saves `metrics` under `key`. */
  override def save(key: ResultKey, metrics: Map[String, Double]): Unit = synchronized {
    val existingIdx = entries.indexWhere(_._1 == key)
    if (existingIdx >= 0) {
      entries.update(existingIdx, (key, metrics))
    } else {
      if (entries.length >= maxEntries) {
        throw DataQualityException(
          "repository_full",
          s"MetricsRepository reached maximum capacity of $maxEntries entries",
        )
      }
      val _ = entries += ((key, metrics))
    }
  }

  /** Returns all historical values for `metric` with `epochMillis < before`. */
  override def history(metric: String, before: Long): Vector[(ResultKey, Double)] = synchronized {
    entries.iterator
      .filter { case (key, map) => key.epochMillis < before && map.contains(metric) }
      .map { case (key, map) => (key, map(metric)) }
      .toVector
      .sortBy(_._1.epochMillis)
  }

  /** Current number of stored [[ResultKey]] entries. */
  def size: Int = synchronized(entries.length)
}
