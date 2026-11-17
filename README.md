# spark-data-quality

Deequ-style data quality checks in Scala 3 with mergeable per-partition analyzer states, metric-history anomaly detection, and byte-stable JSON and Markdown verification reports. This library is designed to run locally or as the engine-agnostic core of a distributed system.

## Problem

Data teams find nulls, duplicate keys, and out-of-range values only after a bad load has reached production, because validation logic often lives in ad hoc scripts or requires heavy distributed infrastructure (like a Spark cluster) even for simple validation. This makes running continuous quality checks expensive and difficult to integrate into CI/CD pipelines.

## Usage

```scala
import io.github.trysac.spark_data_quality.*

val schema = Schema(Vector("order_id" -> ColumnType.Text, "amount" -> ColumnType.Float64))
val csv = "order_id,amount\nORD-1,25.50\nORD-2,40.00\n"
val source = CsvSource.fromString(csv, schema)
val check = Check(Level.Error, "orders")
  .isUnique("order_id")
  .hasMin("amount", Assertion.gte(0.0))
val result = VerificationSuite.run(source, Seq(check))
println(Report.toJson(result))
```

## Public API Summary

- **Analyzers**: `SizeAnalyzer`, `CompletenessAnalyzer`, `UniquenessAnalyzer`, `MinAnalyzer`, `MaxAnalyzer`, `MeanAnalyzer`, `PatternMatchAnalyzer`, `IsContainedInAnalyzer`.
- **Verification**: `VerificationSuite.run` evaluates constraints and anomaly checks in a single pass.
- **Reporting**: `Report.toJson` and `Report.toMarkdown` generate deterministic outputs.
- **Anomalies**: `RelativeRateOfChange` and `ZScore` strategies via `AnomalyCheck`.
- **Repository**: `InMemoryMetricsRepository` maintains historical metrics.
- **Errors**: `DataQualityException` reports specific error codes: `'unknown_column'`, `'type_mismatch'`, `'invalid_regex'`, `'csv_syntax'`, `'input_too_large'`, `'state_too_large'`, `'insufficient_history'`, `'repository_full'`, `'empty_dataset'`.

## Why not Deequ?

Deequ requires a Spark cluster and currently lacks native Scala 3 support. This library provides an engine-agnostic implementation of the Deequ core (mergeable analyzer states), works in Scala 3, allows for testing without distributed infrastructure, and produces byte-stable, deterministic reports.

See [docs/SECURITY.md](docs/SECURITY.md) for security considerations.
