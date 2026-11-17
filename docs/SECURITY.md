# Security & Threat Model

`spark-data-quality` processes potentially untrusted CSV strings and schema definitions in memory. The library does not perform any network I/O or filesystem access.

## Controls

- **Input Size Limitation**: `CsvSource.fromString` enforces a limit of 8,388,608 bytes. Inputs exceeding this throw `DataQualityException('input_too_large')`.
- **Memory Exhaustion**: 
  - `UniquenessAnalyzer` caps distinct value counts at 1,000,000; exceeding this throws `DataQualityException('state_too_large')`.
  - `InMemoryMetricsRepository` limits storage to 10,000 entries; exceeding this throws `DataQualityException('repository_full')`.
- **Error Sanitization**: `DataQualityException.sanitizeSnippet` truncates all untrusted inputs (column names, regex patterns, values) included in error messages to 200 characters, preventing large-payload log injection.

## Defended Threats

- Denial of Service via malicious CSV input size.
- Denial of Service via unbounded state growth (uniqueness maps, metric repository).
- Log injection via overlong error messages.
