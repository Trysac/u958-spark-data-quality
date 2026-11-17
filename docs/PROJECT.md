# Project record

## Objective

Provide an engine-independent, Deequ-style data quality engine in Scala 3, featuring mergeable per-partition analyzer states, historical anomaly detection, and deterministic verification reporting.

## Scope

- The public API as documented, including: `Analyzer`, `Check`, `Assertion`, `VerificationSuite`, `MetricsRepository`, `Report`, `InMemorySource`, and `CsvSource`.
- Comprehensive unit and property-based testing.

## Out-of-scope

- Distributed engine adapters (Spark, Flink, etc.).
- Approximate, non-exact analyzers (e.g., HyperLogLog, KLL).
- Constraint suggestion engines.
- Persistent (non-memory) storage backends.
- Seasonal or complex predictive anomaly detection strategies.
- CLI tools.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created. |

## Effort metrics

| Metric | Value |
|---|---|
| Tokens (input / output) | 4133205 / 62700 |
| Hours (human) | pending measurement |
| Cost (USD) | 1.3455 |

## Factory run u958 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-17 | generation | google/gemini-4-argon | yes | 6487 | 27309 | 0 | 0 | 0.5721 |
| 2026-11-17 | fix-1 | google/gemini-3.8-flash | no | 160119 | 12640 | 0 | 1289009 | 0.2642 |
| 2026-11-17 | fix-2 | google/gemini-3.8-flash | no | 124427 | 19425 | 0 | 1499555 | 0.2786 |
| 2026-11-17 | fix-3 | google/gemini-3.8-flash | no | 52703 | 365 | 0 | 170460 | 0.0537 |
| 2026-11-17 | audit | google/gemini-3.8-flash | no | 152150 | 1470 | 0 | 654686 | 0.1687 |
| 2026-11-17 | docs | google/gemini-3.1-flash-lite | no | 23609 | 1491 | 0 | 0 | 0.0081 |

Total: $1.3455. Prices: vertex line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
