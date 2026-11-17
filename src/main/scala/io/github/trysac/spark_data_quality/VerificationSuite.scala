package io.github.trysac.spark_data_quality

import scala.collection.mutable.{LinkedHashMap, LinkedHashSet}

/** Outcome status of an individual constraint. */
enum ConstraintStatus {

  /** The metric satisfied the assertion. */
  case Success

  /** The metric violated the assertion. */
  case Failure
}

/** Overall verification status of a [[Check]] or [[VerificationResult]]. */
enum VerificationStatus {

  /** Every constraint and anomaly check succeeded. */
  case Success

  /** At least one `Level.Warning` check failed, and no `Level.Error` check failed. */
  case Warning

  /** At least one `Level.Error` check failed. */
  case Error
}

/** Evaluation result of a single [[Constraint]] within a [[Check]].
  *
  * @param checkName
  *   name of the enclosing [[Check]]
  * @param constraint
  *   canonical constraint description (e.g. `"completeness(email) >= 0.999"`)
  * @param status
  *   `Success` or `Failure`
  * @param value
  *   raw computed metric value
  */
final case class ConstraintResult(
    checkName: String,
    constraint: String,
    status: ConstraintStatus,
    value: Double,
)

/** Evaluation result of a [[Check]].
  *
  * @param check
  *   the evaluated check definition
  * @param status
  *   status of this check (`Success`, `Warning`, or `Error`)
  * @param constraintResults
  *   per-constraint outcomes in declaration order
  */
final case class CheckResult(
    check: Check,
    status: VerificationStatus,
    constraintResults: Vector[ConstraintResult],
)

/** Complete result of [[VerificationSuite.run]].
  *
  * @param status
  *   overall verification status (`"Success"`, `"Warning"`, or `"Error"`)
  * @param checkResults
  *   per-check outcomes
  * @param constraintResults
  *   flattened per-constraint outcomes across all checks and anomaly checks
  * @param metrics
  *   map from canonical metric name to raw computed `Double` value
  */
final case class VerificationResult(
    status: String,
    checkResults: Vector[CheckResult],
    constraintResults: Vector[ConstraintResult],
    metrics: Map[String, Double],
) {

  /** Typed enum representation of [[status]]. */
  def verificationStatus: VerificationStatus = VerificationStatus.valueOf(status)
}

/** Executes data quality [[Check]]s and [[AnomalyCheck]]s against a [[DataSource]] in a single pass
  * per partition.
  */
object VerificationSuite {

  private val MetricNamePattern = "^([a-zA-Z]+)(?:\\((.+)\\))?$".r

  /** Runs all `checks` and `anomalyChecks` against `data`.
    *
    * Each partition of `data` is materialized once per partition so all distinct analyzers compute
    * their per-partition states in a single pass, and partition states are merged in partition
    * order. When `repository` is provided, historical anomaly checks are evaluated against entries
    * before `key` (or `Long.MaxValue` if `key` is omitted), and the newly computed metrics are
    * saved under `key` when `key` is provided.
    */
  def run(
      data: DataSource,
      checks: Seq[Check],
      repository: Option[MetricsRepository] = None,
      key: Option[ResultKey] = None,
      anomalyChecks: Seq[AnomalyCheck] = Nil,
  ): VerificationResult = {
    val schema = data.schema

    // Collect distinct analyzers in deterministic encounter order
    val analyzersSet = LinkedHashSet.empty[Analyzer]
    for {
      check <- checks
      constraint <- check.constraints
    } {
      constraint.analyzer.validate(schema)
      val _ = analyzersSet += constraint.analyzer
    }
    for (ac <- anomalyChecks) {
      if (!analyzersSet.exists(_.metricName == ac.metric)) {
        val inferred = inferAnalyzerForMetric(ac.metric, schema)
        inferred.validate(schema)
        val _ = analyzersSet += inferred
      }
    }
    val analyzers = analyzersSet.toVector

    // Compute per-partition states in a single pass per partition, then merge in partition order
    val initialStates: Vector[AnalyzerState] = analyzers.map(_.zeroState)
    val mergedStates: Vector[AnalyzerState] = data.partitions.foldLeft(initialStates) {
      (currentStates, partitionIter) =>
        val partitionRows = partitionIter.toVector
        if (partitionRows.isEmpty) {
          currentStates
        } else {
          analyzers.indices.toVector.map { i =>
            val analyzer = analyzers(i)
            val partState = analyzer.computePartition(partitionRows, schema)
            analyzer.mergeUntyped(currentStates(i), partState)
          }
        }
    }

    // Finalize metrics
    val metricMap = LinkedHashMap.empty[String, Double]
    for (idx <- analyzers.indices) {
      val analyzer = analyzers(idx)
      val value = analyzer.metricUntyped(mergedStates(idx))
      metricMap.put(analyzer.metricName, value)
    }
    val computedMetrics = metricMap.toMap

    // Evaluate standard checks
    val checkResultsBuilder = Vector.newBuilder[CheckResult]
    val allConstraintResults = Vector.newBuilder[ConstraintResult]

    for (check <- checks) {
      val cResults = check.constraints.map { constraint =>
        val value = computedMetrics(constraint.analyzer.metricName)
        val passed = constraint.assertion(value)
        val cStatus = if (passed) ConstraintStatus.Success else ConstraintStatus.Failure
        ConstraintResult(check.name, constraint.description, cStatus, value)
      }
      val anyFailed = cResults.exists(_.status == ConstraintStatus.Failure)
      val chkStatus =
        if (!anyFailed) VerificationStatus.Success
        else if (check.level == Level.Error) VerificationStatus.Error
        else VerificationStatus.Warning
      val _ = checkResultsBuilder += CheckResult(check, chkStatus, cResults)
      val _ = allConstraintResults ++= cResults
    }

    // Evaluate anomaly checks against repository history
    if (anomalyChecks.nonEmpty) {
      val repo = repository.getOrElse(
        throw DataQualityException(
          "insufficient_history",
          "Anomaly checks require a MetricsRepository",
        ),
      )
      val beforeTs = key.map(_.epochMillis).getOrElse(Long.MaxValue)
      for (ac <- anomalyChecks) {
        val currentVal = computedMetrics.getOrElse(
          ac.metric,
          throw DataQualityException(
            "unknown_metric",
            s"Metric '${DataQualityException.sanitizeSnippet(ac.metric)}' was not computed",
          ),
        )
        val historyPoints = repo.history(ac.metric, beforeTs).map(_._2)
        val normal = ac.strategy.isNormal(ac.metric, historyPoints, currentVal)
        val cStatus = if (normal) ConstraintStatus.Success else ConstraintStatus.Failure
        val desc = s"${ac.metric} ${ac.strategy.description}"
        val cr = ConstraintResult("anomaly", desc, cStatus, currentVal)
        val chkStatus =
          if (normal) VerificationStatus.Success
          else if (ac.level == Level.Error) VerificationStatus.Error
          else VerificationStatus.Warning
        val syntheticCheck = Check(ac.level, "anomaly", Vector.empty)
        val _ = checkResultsBuilder += CheckResult(syntheticCheck, chkStatus, Vector(cr))
        val _ = allConstraintResults += cr
      }
    }

    // Save metrics to repository if both repository and key are provided
    for {
      repo <- repository
      resultKey <- key
    } repo.save(resultKey, computedMetrics)

    val finalChecks = checkResultsBuilder.result()
    val overallStatus =
      if (finalChecks.exists(_.status == VerificationStatus.Error)) VerificationStatus.Error
      else if (finalChecks.exists(_.status == VerificationStatus.Warning))
        VerificationStatus.Warning
      else VerificationStatus.Success

    VerificationResult(
      status = overallStatus.toString,
      checkResults = finalChecks,
      constraintResults = allConstraintResults.result(),
      metrics = computedMetrics,
    )
  }

  private def inferAnalyzerForMetric(metric: String, schema: Schema): Analyzer =
    metric match {
      case "size"                               => SizeAnalyzer
      case MetricNamePattern("completeness", c) => CompletenessAnalyzer(c)
      case MetricNamePattern("uniqueness", c)   => UniquenessAnalyzer(c)
      case MetricNamePattern("minimum", c)      => MinAnalyzer(c)
      case MetricNamePattern("maximum", c)      => MaxAnalyzer(c)
      case MetricNamePattern("mean", c)         => MeanAnalyzer(c)
      case _                                    =>
        throw DataQualityException(
          "unknown_column",
          s"Cannot infer analyzer for metric '${DataQualityException.sanitizeSnippet(metric)}' on schema $schema",
        )
    }
}
