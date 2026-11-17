package io.github.trysac.spark_data_quality

/** Strategy for evaluating whether a current metric value is anomalous given historical points. */
sealed trait AnomalyStrategy {

  /** Human-readable strategy description for reports. */
  def description: String

  /** Returns `true` when `currentValue` is within normal bounds given `historyValues` (ordered by
    * ascending timestamp), or `false` when it is an anomaly.
    */
  def isNormal(metric: String, historyValues: Vector[Double], currentValue: Double): Boolean
}

/** Compares the current value against the latest historical point's value.
  *
  * Flags an anomaly if the relative change `(current - previous) / |previous|` falls below
  * `-maxDecrease` or exceeds `+maxIncrease`.
  *
  * @param maxDecrease
  *   maximum allowed relative decrease (e.g. `0.5` allows up to a 50% drop)
  * @param maxIncrease
  *   maximum allowed relative increase (e.g. `2.0` allows up to a 200% increase)
  */
final case class RelativeRateOfChange(maxDecrease: Double, maxIncrease: Double)
    extends AnomalyStrategy {

  if (maxDecrease < 0.0 || maxIncrease < 0.0) {
    throw DataQualityException(
      "invalid_strategy",
      s"maxDecrease ($maxDecrease) and maxIncrease ($maxIncrease) must be non-negative",
    )
  }

  /** Human-readable strategy description for reports. */
  override def description: String =
    s"RelativeRateOfChange(maxDecrease = $maxDecrease, maxIncrease = $maxIncrease)"

  /** Evaluates whether `currentValue` is within allowed relative change bounds. */
  override def isNormal(
      metric: String,
      historyValues: Vector[Double],
      currentValue: Double,
  ): Boolean = {
    if (historyValues.isEmpty) {
      throw DataQualityException(
        "insufficient_history",
        s"RelativeRateOfChange on '${DataQualityException.sanitizeSnippet(metric)}' requires at least 1 history point, found 0",
      )
    }
    val previous = historyValues.last
    if (previous == 0.0) {
      currentValue == 0.0
    } else {
      val denom = math.abs(previous)
      val minAllowed = previous - maxDecrease * denom
      val maxAllowed = previous + maxIncrease * denom
      val eps = 1e-12
      currentValue + eps >= minAllowed && currentValue - eps <= maxAllowed
    }
  }
}

/** Compares the current value against the mean and population standard deviation of all historical
  * points.
  *
  * Requires at least 3 historical points; otherwise raises [[DataQualityException]] with code
  * `'insufficient_history'`.
  *
  * @param maxDeviations
  *   maximum allowed absolute Z-score (`|current - mean| / stdDev <= maxDeviations`)
  */
final case class ZScore(maxDeviations: Double) extends AnomalyStrategy {

  if (maxDeviations <= 0.0) {
    throw DataQualityException(
      "invalid_strategy",
      s"maxDeviations must be > 0, got $maxDeviations",
    )
  }

  /** Human-readable strategy description for reports. */
  override def description: String = s"ZScore(maxDeviations = $maxDeviations)"

  /** Evaluates whether `currentValue` is within `maxDeviations` standard deviations of history. */
  override def isNormal(
      metric: String,
      historyValues: Vector[Double],
      currentValue: Double,
  ): Boolean = {
    if (historyValues.length < 3) {
      throw DataQualityException(
        "insufficient_history",
        s"ZScore on '${DataQualityException.sanitizeSnippet(metric)}' requires at least 3 history points, found ${historyValues.length}",
      )
    }
    val n = historyValues.length.toDouble
    val mean = historyValues.sum / n
    val variance = historyValues.map(v => (v - mean) * (v - mean)).sum / n
    val stdDev = math.sqrt(variance)
    val diff = math.abs(currentValue - mean)
    if (stdDev == 0.0) {
      diff <= 1e-12
    } else {
      (diff / stdDev) <= maxDeviations + 1e-12
    }
  }
}

/** Anomaly check configuration over a named metric.
  *
  * @param metric
  *   canonical metric name (e.g. `"size"` or `"completeness(email)"`)
  * @param strategy
  *   anomaly detection strategy
  * @param level
  *   severity level when an anomaly is flagged
  */
final case class AnomalyCheck(
    metric: String,
    strategy: AnomalyStrategy,
    level: Level = Level.Warning,
)
