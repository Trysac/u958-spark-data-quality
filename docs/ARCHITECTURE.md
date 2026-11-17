# Architecture

`spark-data-quality` separates tabular data representation and partitioned scanning from metric aggregation, constraint evaluation, anomaly detection, and report rendering.

## Core Components

1. **Tabular Model (`Cell`, `ColumnType`, `Schema`)**
   - `Cell` is a Scala 3 enum (`Null`, `Text`, `Int64`, `Float64`, `Bool`).
   - `Schema` resolves column indices and validates cell types at scan time, raising `DataQualityException("unknown_column", ...)` or `DataQualityException("type_mismatch", ...)`.

2. **Partitioned Data Sources (`DataSource`, `InMemorySource`, `CsvSource`)**
   - `DataSource` exposes `schema: Schema` and `partitions: Seq[Iterator[IndexedSeq[Cell]]]`, mirroring distributed `mapPartitions` execution.
   - `CsvSource.fromString` parses RFC 4180 CSV text (supporting CRLF/LF newlines, quoted fields, doubled quotes, and empty fields as `Cell.Null`) while enforcing a default 8 MiB UTF-8 byte limit (`input_too_large`).

3. **Analyzers and Mergeable States (`Analyzer`, `AnalyzerState`)**
   - Every analyzer defines `State <: AnalyzerState`, `computePartition`, `zeroState`, `merge(a, b)`, and `metric(state)`.
   - `merge` is associative and commutative so partition states can be combined in any order or tree reduction without changing the result.
   - `UniquenessAnalyzer` caps its frequency map at 1,000,000 distinct values (`state_too_large`) to bound heap consumption.

4. **Verification Suite (`VerificationSuite`, `Check`, `Constraint`, `Assertion`)**
   - Collects all unique analyzers required by the requested checks and anomaly checks.
   - Scans each partition once to compute per-partition states, merges them across partitions, and evaluates all assertions and historical anomaly strategies.

5. **Metrics Repository and Anomaly Detection (`MetricsRepository`, `RelativeRateOfChange`, `ZScore`)**
   - `InMemoryMetricsRepository` stores up to `maxEntries = 10_000` timestamped `ResultKey` runs (`repository_full` on overflow).
   - `RelativeRateOfChange` compares against the latest historical point; `ZScore` computes population standard deviation over all historical points (requiring at least 3 points, or raising `insufficient_history`).

6. **Reporting (`Report`)**
   - `Report.toJson` and `Report.toMarkdown` sort metric keys lexicographically, preserve constraint declaration order, and round floating-point values to 4 decimal places with `RoundingMode.HALF_EVEN` for byte-identical output across runs.
