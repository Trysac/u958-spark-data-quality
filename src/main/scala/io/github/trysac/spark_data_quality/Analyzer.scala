package io.github.trysac.spark_data_quality

import java.util.regex.{Pattern, PatternSyntaxException}

/** Mergeable analyzer state for single-pass partitioned metric computation. */
sealed trait AnalyzerState

/** State for [[SizeAnalyzer]]. */
final case class SizeState(count: Long) extends AnalyzerState

/** State for [[CompletenessAnalyzer]]. */
final case class CompletenessState(nonNullCount: Long, totalCount: Long) extends AnalyzerState

/** State for [[UniquenessAnalyzer]]. */
final case class UniquenessState(frequencies: Map[Cell, Long], nonNullCount: Long)
    extends AnalyzerState

/** State for [[MinAnalyzer]]. */
final case class MinState(min: Option[Double]) extends AnalyzerState

/** State for [[MaxAnalyzer]]. */
final case class MaxState(max: Option[Double]) extends AnalyzerState

/** State for [[MeanAnalyzer]]. */
final case class MeanState(sum: Double, count: Long) extends AnalyzerState

/** State for [[PatternMatchAnalyzer]] and [[IsContainedInAnalyzer]]. */
final case class RatioState(matchingCount: Long, totalRows: Long) extends AnalyzerState

/** Single-pass analyzer with an associative, commutative state `merge` operation. */
sealed trait Analyzer {

  /** State type produced by this analyzer. */
  type State <: AnalyzerState

  /** Canonical metric name used in reports and [[MetricsRepository]]. */
  def metricName: String

  /** Validates that this analyzer can run against `schema`, raising [[DataQualityException]] if
    * not.
    */
  def validate(schema: Schema): Unit

  /** Computes this analyzer's state over a single partition's rows. */
  def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): State

  /** Returns the identity state for merging. */
  def zeroState: State

  /** Associative and commutative merge of two partition states. */
  def merge(a: State, b: State): State

  /** Finalizes the merged state into a `Double` metric value. */
  def metric(state: State): Double

  /** Untyped bridge for merging states of identical analyzer instances. */
  private[spark_data_quality] final def mergeUntyped(
      a: AnalyzerState,
      b: AnalyzerState,
  ): AnalyzerState =
    (this, a, b) match {
      case (SizeAnalyzer, s1: SizeState, s2: SizeState) =>
        SizeAnalyzer.merge(s1, s2)
      case (c: CompletenessAnalyzer, s1: CompletenessState, s2: CompletenessState) =>
        c.merge(s1, s2)
      case (u: UniquenessAnalyzer, s1: UniquenessState, s2: UniquenessState) =>
        u.merge(s1, s2)
      case (m: MinAnalyzer, s1: MinState, s2: MinState) =>
        m.merge(s1, s2)
      case (m: MaxAnalyzer, s1: MaxState, s2: MaxState) =>
        m.merge(s1, s2)
      case (m: MeanAnalyzer, s1: MeanState, s2: MeanState) =>
        m.merge(s1, s2)
      case (p: PatternMatchAnalyzer, s1: RatioState, s2: RatioState) =>
        p.merge(s1, s2)
      case (ci: IsContainedInAnalyzer, s1: RatioState, s2: RatioState) =>
        ci.merge(s1, s2)
      case _ =>
        throw DataQualityException(
          "invalid_state",
          s"Incompatible state types for analyzer $metricName",
        )
    }

  /** Untyped bridge for evaluating metric from an [[AnalyzerState]]. */
  private[spark_data_quality] final def metricUntyped(s: AnalyzerState): Double =
    (this, s) match {
      case (SizeAnalyzer, st: SizeState)                    => SizeAnalyzer.metric(st)
      case (c: CompletenessAnalyzer, st: CompletenessState) => c.metric(st)
      case (u: UniquenessAnalyzer, st: UniquenessState)     => u.metric(st)
      case (m: MinAnalyzer, st: MinState)                   => m.metric(st)
      case (m: MaxAnalyzer, st: MaxState)                   => m.metric(st)
      case (m: MeanAnalyzer, st: MeanState)                 => m.metric(st)
      case (p: PatternMatchAnalyzer, st: RatioState)        => p.metric(st)
      case (ci: IsContainedInAnalyzer, st: RatioState)      => ci.metric(st)
      case _                                                =>
        throw DataQualityException(
          "invalid_state",
          s"Incompatible state type for analyzer $metricName",
        )
    }
}

/** Constants and utilities for analyzers. */
object Analyzer {

  /** Maximum distinct values allowed in a [[UniquenessState]]. */
  val MaxUniqueDistinctValues: Int = 1_000_000

  /** Extracts a finite numeric value (`Some(double)`) from a cell, or `None` for Null / NaN. */
  private[spark_data_quality] def extractFiniteNumeric(
      column: String,
      colType: ColumnType,
      cell: Cell,
  ): Option[Double] = {
    cell match {
      case Cell.Null                                    => None
      case Cell.Int64(v) if colType == ColumnType.Int64 =>
        Some(v.toDouble)
      case Cell.Float64(v) if colType == ColumnType.Float64 =>
        if (v.isNaN) None else Some(v)
      case _ =>
        throw DataQualityException(
          "type_mismatch",
          s"Column '${DataQualityException.sanitizeSnippet(column)}' has incompatible cell $cell for $colType",
        )
    }
  }

  /** Verifies that `colType` is numeric (`Int64` or `Float64`). */
  private[spark_data_quality] def requireNumericColumn(
      analyzerName: String,
      column: String,
      colType: ColumnType,
  ): Unit = {
    colType match {
      case ColumnType.Int64 | ColumnType.Float64 => ()
      case other                                 =>
        throw DataQualityException(
          "type_mismatch",
          s"Analyzer '$analyzerName' requires Int64 or Float64 column, but '${DataQualityException.sanitizeSnippet(column)}' is $other",
        )
    }
  }
}

/** Computes total row count (`size`). */
case object SizeAnalyzer extends Analyzer {

  /** State type for row count. */
  override type State = SizeState

  /** Canonical metric name. */
  override val metricName: String = "size"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = ()

  /** Empty zero state. */
  override def zeroState: SizeState = SizeState(0L)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): SizeState =
    SizeState(rows.size.toLong)

  /** Merges partition states. */
  override def merge(a: SizeState, b: SizeState): SizeState =
    SizeState(a.count + b.count)

  /** Returns computed metric value. */
  override def metric(state: SizeState): Double =
    state.count.toDouble
}

/** Computes completeness (`nonNullRows / totalRows`) of a column. */
final case class CompletenessAnalyzer(column: String) extends Analyzer {

  /** State type for completeness. */
  override type State = CompletenessState

  /** Canonical metric name. */
  override val metricName: String = s"completeness($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val _ = schema.resolve(column)
  }

  /** Empty zero state. */
  override def zeroState: CompletenessState = CompletenessState(0L, 0L)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): CompletenessState = {
    val (idx, colType) = schema.resolve(column)
    val (nonNull, total) = rows.foldLeft((0L, 0L)) { case ((nn, tot), row) =>
      val cell = row(idx)
      schema.validateCell(column, colType, cell)
      val newNn = if (cell != Cell.Null) nn + 1L else nn
      (newNn, tot + 1L)
    }
    CompletenessState(nonNull, total)
  }

  /** Merges partition states. */
  override def merge(a: CompletenessState, b: CompletenessState): CompletenessState =
    CompletenessState(a.nonNullCount + b.nonNullCount, a.totalCount + b.totalCount)

  /** Returns computed metric value. */
  override def metric(state: CompletenessState): Double = {
    if (state.totalCount == 0L) {
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName over 0 rows",
      )
    }
    state.nonNullCount.toDouble / state.totalCount.toDouble
  }
}

/** Computes uniqueness (`uniqueNonNullRows / nonNullRows`) of a column.
  *
  * A value is unique if its total occurrence count across all partitions is 1.
  */
final case class UniquenessAnalyzer(
    column: String,
    maxDistinctValues: Int = Analyzer.MaxUniqueDistinctValues,
) extends Analyzer {

  /** State type for uniqueness. */
  override type State = UniquenessState

  /** Canonical metric name. */
  override val metricName: String = s"uniqueness($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val _ = schema.resolve(column)
  }

  /** Empty zero state. */
  override def zeroState: UniquenessState = UniquenessState(Map.empty, 0L)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): UniquenessState = {
    val (idx, colType) = schema.resolve(column)
    val (counts, nonNull) = rows.foldLeft((Map.empty[Cell, Long], 0L)) { case ((accMap, nn), row) =>
      val cell = row(idx)
      schema.validateCell(column, colType, cell)
      if (cell != Cell.Null) {
        val prev = accMap.getOrElse(cell, 0L)
        if (prev == 0L && accMap.size >= maxDistinctValues) {
          throw DataQualityException(
            "state_too_large",
            s"Uniqueness state for '${DataQualityException.sanitizeSnippet(column)}' exceeded $maxDistinctValues distinct values",
          )
        }
        (accMap.updated(cell, prev + 1L), nn + 1L)
      } else {
        (accMap, nn)
      }
    }
    UniquenessState(counts, nonNull)
  }

  /** Merges partition states. */
  override def merge(a: UniquenessState, b: UniquenessState): UniquenessState = {
    if (a.frequencies.isEmpty) b
    else if (b.frequencies.isEmpty) a
    else {
      val (larger, smaller) =
        if (a.frequencies.size >= b.frequencies.size) (a.frequencies, b.frequencies)
        else (b.frequencies, a.frequencies)
      val merged = smaller.foldLeft(larger) { case (accMap, (cell, count)) =>
        accMap.get(cell) match {
          case Some(existing) =>
            accMap.updated(cell, existing + count)
          case None =>
            if (accMap.size >= maxDistinctValues) {
              throw DataQualityException(
                "state_too_large",
                s"Uniqueness state for '${DataQualityException.sanitizeSnippet(column)}' exceeded $maxDistinctValues distinct values",
              )
            }
            accMap.updated(cell, count)
        }
      }
      UniquenessState(merged, a.nonNullCount + b.nonNullCount)
    }
  }

  /** Returns computed metric value. */
  override def metric(state: UniquenessState): Double = {
    if (state.nonNullCount == 0L) {
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName when column has 0 non-null rows",
      )
    }
    val uniqueCount = state.frequencies.valuesIterator.count(_ == 1L).toLong
    uniqueCount.toDouble / state.nonNullCount.toDouble
  }
}

/** Computes minimum value of a numeric (`Int64` or `Float64`) column, ignoring nulls and `NaN`. */
final case class MinAnalyzer(column: String) extends Analyzer {

  /** State type for minimum. */
  override type State = MinState

  /** Canonical metric name. */
  override val metricName: String = s"minimum($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val (_, colType) = schema.resolve(column)
    Analyzer.requireNumericColumn("minimum", column, colType)
  }

  /** Empty zero state. */
  override def zeroState: MinState = MinState(None)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): MinState = {
    val (idx, colType) = schema.resolve(column)
    Analyzer.requireNumericColumn("minimum", column, colType)
    val currentMin = rows.foldLeft(Option.empty[Double]) { (accOpt, row) =>
      Analyzer.extractFiniteNumeric(column, colType, row(idx)) match {
        case Some(v) =>
          accOpt match {
            case Some(m) => Some(math.min(m, v))
            case None    => Some(v)
          }
        case None => accOpt
      }
    }
    MinState(currentMin)
  }

  /** Merges partition states. */
  override def merge(a: MinState, b: MinState): MinState =
    (a.min, b.min) match {
      case (Some(x), Some(y)) => MinState(Some(math.min(x, y)))
      case (Some(x), None)    => MinState(Some(x))
      case (None, Some(y))    => MinState(Some(y))
      case (None, None)       => MinState(None)
    }

  /** Returns computed metric value. */
  override def metric(state: MinState): Double =
    state.min.getOrElse(
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName with 0 numeric values",
      ),
    )
}

/** Computes maximum value of a numeric (`Int64` or `Float64`) column, ignoring nulls and `NaN`. */
final case class MaxAnalyzer(column: String) extends Analyzer {

  /** State type for maximum. */
  override type State = MaxState

  /** Canonical metric name. */
  override val metricName: String = s"maximum($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val (_, colType) = schema.resolve(column)
    Analyzer.requireNumericColumn("maximum", column, colType)
  }

  /** Empty zero state. */
  override def zeroState: MaxState = MaxState(None)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): MaxState = {
    val (idx, colType) = schema.resolve(column)
    Analyzer.requireNumericColumn("maximum", column, colType)
    val currentMax = rows.foldLeft(Option.empty[Double]) { (accOpt, row) =>
      Analyzer.extractFiniteNumeric(column, colType, row(idx)) match {
        case Some(v) =>
          accOpt match {
            case Some(m) => Some(math.max(m, v))
            case None    => Some(v)
          }
        case None => accOpt
      }
    }
    MaxState(currentMax)
  }

  /** Merges partition states. */
  override def merge(a: MaxState, b: MaxState): MaxState =
    (a.max, b.max) match {
      case (Some(x), Some(y)) => MaxState(Some(math.max(x, y)))
      case (Some(x), None)    => MaxState(Some(x))
      case (None, Some(y))    => MaxState(Some(y))
      case (None, None)       => MaxState(None)
    }

  /** Returns computed metric value. */
  override def metric(state: MaxState): Double =
    state.max.getOrElse(
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName with 0 numeric values",
      ),
    )
}

/** Computes arithmetic mean of a numeric (`Int64` or `Float64`) column, ignoring nulls and `NaN`.
  */
final case class MeanAnalyzer(column: String) extends Analyzer {

  /** State type for mean. */
  override type State = MeanState

  /** Canonical metric name. */
  override val metricName: String = s"mean($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val (_, colType) = schema.resolve(column)
    Analyzer.requireNumericColumn("mean", column, colType)
  }

  /** Empty zero state. */
  override def zeroState: MeanState = MeanState(0.0, 0L)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): MeanState = {
    val (idx, colType) = schema.resolve(column)
    Analyzer.requireNumericColumn("mean", column, colType)
    val (sum, count) = rows.foldLeft((0.0, 0L)) { case ((s, c), row) =>
      Analyzer.extractFiniteNumeric(column, colType, row(idx)) match {
        case Some(v) => (s + v, c + 1L)
        case None    => (s, c)
      }
    }
    MeanState(sum, count)
  }

  /** Merges partition states. */
  override def merge(a: MeanState, b: MeanState): MeanState =
    MeanState(a.sum + b.sum, a.count + b.count)

  /** Returns computed metric value. */
  override def metric(state: MeanState): Double = {
    if (state.count == 0L) {
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName with 0 numeric values",
      )
    }
    state.sum / state.count.toDouble
  }
}

/** Computes fraction of non-null `Text` values in `column` that fully match `regex`. */
final case class PatternMatchAnalyzer(column: String, regex: String) extends Analyzer {

  /** State type for pattern matching ratio. */
  override type State = RatioState

  private val compiledPattern: Pattern = {
    try {
      Pattern.compile(regex)
    } catch {
      case e: PatternSyntaxException =>
        throw DataQualityException(
          "invalid_regex",
          s"Invalid regular expression '${DataQualityException.sanitizeSnippet(regex)}': ${DataQualityException.sanitizeSnippet(Option(e.getDescription).getOrElse("syntax error"))}",
        )
    }
  }

  /** Canonical metric name. */
  override val metricName: String = s"patternMatch($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val (_, colType) = schema.resolve(column)
    if (colType != ColumnType.Text) {
      throw DataQualityException(
        "type_mismatch",
        s"Analyzer 'patternMatch' requires Text column, but '${DataQualityException.sanitizeSnippet(column)}' is $colType",
      )
    }
  }

  /** Empty zero state. */
  override def zeroState: RatioState = RatioState(0L, 0L)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): RatioState = {
    val (idx, colType) = schema.resolve(column)
    validate(schema)
    val (matching, nonNull) = rows.foldLeft((0L, 0L)) { case ((m, nn), row) =>
      val cell = row(idx)
      schema.validateCell(column, colType, cell)
      cell match {
        case Cell.Null    => (m, nn)
        case Cell.Text(s) =>
          val newM = if (compiledPattern.matcher(s).matches()) m + 1L else m
          (newM, nn + 1L)
        case _ => (m, nn)
      }
    }
    RatioState(matching, nonNull)
  }

  /** Merges partition states. */
  override def merge(a: RatioState, b: RatioState): RatioState =
    RatioState(a.matchingCount + b.matchingCount, a.totalRows + b.totalRows)

  /** Returns computed metric value. */
  override def metric(state: RatioState): Double = {
    if (state.totalRows == 0L) {
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName when column has 0 non-null values",
      )
    }
    state.matchingCount.toDouble / state.totalRows.toDouble
  }
}

/** Computes fraction of non-null values in `column` whose string form is in `allowed`. */
final case class IsContainedInAnalyzer(column: String, allowed: Set[String]) extends Analyzer {

  /** State type for set containment ratio. */
  override type State = RatioState

  /** Canonical metric name. */
  override val metricName: String = s"isContainedIn($column)"

  /** Validates schema. */
  override def validate(schema: Schema): Unit = {
    val _ = schema.resolve(column)
  }

  /** Empty zero state. */
  override def zeroState: RatioState = RatioState(0L, 0L)

  /** Computes partition state. */
  override def computePartition(rows: Seq[IndexedSeq[Cell]], schema: Schema): RatioState = {
    val (idx, colType) = schema.resolve(column)
    val (matching, nonNull) = rows.foldLeft((0L, 0L)) { case ((m, nn), row) =>
      val cell = row(idx)
      schema.validateCell(column, colType, cell)
      cell.asString match {
        case Some(s) =>
          val newM = if (allowed.contains(s)) m + 1L else m
          (newM, nn + 1L)
        case None => (m, nn)
      }
    }
    RatioState(matching, nonNull)
  }

  /** Merges partition states. */
  override def merge(a: RatioState, b: RatioState): RatioState =
    RatioState(a.matchingCount + b.matchingCount, a.totalRows + b.totalRows)

  /** Returns computed metric value. */
  override def metric(state: RatioState): Double = {
    if (state.totalRows == 0L) {
      throw DataQualityException(
        "empty_dataset",
        s"Cannot compute $metricName when column has 0 non-null values",
      )
    }
    state.matchingCount.toDouble / state.totalRows.toDouble
  }
}
