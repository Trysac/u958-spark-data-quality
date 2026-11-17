# Engine-independent single-pass mergeable analyzer states over partitioned DataSources

Status: accepted (generated, pending human review)

## Context

Data teams need Deequ-style data quality checks, historical anomaly detection, and reproducible reports in Scala 3 without spinning up a Spark cluster for unit tests or pulling in hundreds of megabytes of Scala 2.13 Spark dependencies.

## Decision

Model datasets via a partitioned DataSource trait (`Seq[Iterator[IndexedSeq[Cell]]]`) and implement all eight analyzers with pure, associative, and commutative `AnalyzerState` merge operations evaluated in one pass per partition, paired with bounded in-memory CSV parsing and metric history storage.

## Consequences

Every analyzer is deterministically testable in memory and verified by ScalaCheck partition-split properties; reports are byte-identical across runs; a future Spark or Flink adapter can map each partition to `computePartition` and reduce states with `merge`.
