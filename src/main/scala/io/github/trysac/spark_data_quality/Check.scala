package io.github.trysac.spark_data_quality

/** Severity level of a [[Check]] or [[AnomalyCheck]]. */
enum Level {

  /** Warning severity: failing constraints produce overall status `Warning` unless an `Error`
    * fails.
    */
  case Warning

  /** Error severity: any failing constraint produces overall status `Error`. */
  case Error
}

/** Predicate over a computed `Double` metric value with a deterministic description string.
  *
  * @param description
  *   canonical description such as `">= 0.999"` or `"== 1.0"`
  * @param evaluate
  *   predicate function
  */
final case class Assertion(description: String, evaluate: Double => Boolean) {

  /** Returns `true` if `value` satisfies this assertion. */
  def apply(value: Double): Boolean = evaluate(value)
}

/** Factory methods for [[Assertion]]. */
object Assertion {

  private val Epsilon: Double = 1e-12

  private def formatBound(v: Double): String = {
    if (v == v.toLong.toDouble && v.abs < 1e15) {
      val asDoubleStr = v.toString
      if (asDoubleStr.endsWith(".0")) asDoubleStr else s"${v.toLong}.0"
    } else {
      java.math.BigDecimal.valueOf(v).stripTrailingZeros().toPlainString
    }
  }

  /** Asserts that the metric equals `target` (within floating-point tolerance `1e-12`). */
  def eq(target: Double): Assertion =
    Assertion(s"== ${formatBound(target)}", v => !v.isNaN && math.abs(v - target) <= Epsilon)

  /** Asserts that the metric is greater than or equal to `lower`. */
  def gte(lower: Double): Assertion =
    Assertion(s">= ${formatBound(lower)}", v => !v.isNaN && v + Epsilon >= lower)

  /** Asserts that the metric is less than or equal to `upper`. */
  def lte(upper: Double): Assertion =
    Assertion(s"<= ${formatBound(upper)}", v => !v.isNaN && v - Epsilon <= upper)

  /** Asserts that the metric lies in the inclusive interval `[lower, upper]`. */
  def between(lower: Double, upper: Double): Assertion = {
    if (lower > upper) {
      throw DataQualityException(
        "invalid_assertion",
        s"Lower bound $lower must be <= upper bound $upper",
      )
    }
    Assertion(
      s"between ${formatBound(lower)} and ${formatBound(upper)}",
      v => !v.isNaN && v + Epsilon >= lower && v - Epsilon <= upper,
    )
  }
}

/** A single metric constraint pairing an [[Analyzer]] with an [[Assertion]].
  *
  * @param analyzer
  *   the metric analyzer
  * @param assertion
  *   the assertion to evaluate on the analyzer's metric
  */
final case class Constraint(analyzer: Analyzer, assertion: Assertion) {

  /** Canonical constraint label used in reports, e.g. `"completeness(email) >= 0.999"`. */
  def description: String = s"${analyzer.metricName} ${assertion.description}"
}

/** A named collection of data quality constraints sharing a severity [[Level]].
  *
  * @param level
  *   severity level (`Level.Warning` or `Level.Error`)
  * @param name
  *   human-readable check group name
  * @param constraints
  *   ordered vector of constraints
  */
final case class Check(
    level: Level,
    name: String,
    constraints: Vector[Constraint] = Vector.empty,
) {

  private def append(analyzer: Analyzer, assertion: Assertion): Check =
    copy(constraints = constraints :+ Constraint(analyzer, assertion))

  /** Adds a constraint on dataset row count (`size`). */
  def hasSize(assertion: Assertion): Check =
    append(SizeAnalyzer, assertion)

  /** Adds a constraint requiring 100% completeness (`completeness(column) == 1.0`). */
  def isComplete(column: String): Check =
    append(CompletenessAnalyzer(column), Assertion.eq(1.0))

  /** Adds a constraint on the completeness fraction of `column`. */
  def hasCompleteness(column: String, assertion: Assertion): Check =
    append(CompletenessAnalyzer(column), assertion)

  /** Adds a constraint requiring 100% uniqueness (`uniqueness(column) == 1.0`). */
  def isUnique(column: String): Check =
    append(UniquenessAnalyzer(column), Assertion.eq(1.0))

  /** Adds a constraint on the uniqueness fraction of `column`. */
  def hasUniqueness(column: String, assertion: Assertion): Check =
    append(UniquenessAnalyzer(column), assertion)

  /** Adds a constraint on the minimum value of numeric `column`. */
  def hasMin(column: String, assertion: Assertion): Check =
    append(MinAnalyzer(column), assertion)

  /** Adds a constraint on the maximum value of numeric `column`. */
  def hasMax(column: String, assertion: Assertion): Check =
    append(MaxAnalyzer(column), assertion)

  /** Adds a constraint on the mean value of numeric `column`. */
  def hasMean(column: String, assertion: Assertion): Check =
    append(MeanAnalyzer(column), assertion)

  /** Adds a constraint on the fraction of non-null `Text` values in `column` matching `regex`. */
  def hasPattern(
      column: String,
      regex: String,
      assertion: Assertion = Assertion.eq(1.0),
  ): Check =
    append(PatternMatchAnalyzer(column, regex), assertion)

  /** Adds a constraint on the fraction of non-null values in `column` contained in `allowed`. */
  def isContainedIn(
      column: String,
      allowed: Set[String],
      assertion: Assertion = Assertion.eq(1.0),
  ): Check =
    append(IsContainedInAnalyzer(column, allowed), assertion)
}
