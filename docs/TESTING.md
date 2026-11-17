# Testing

## Running

Tests are executed via `sbt test`.

## Policy

- **Coverage**: Statement coverage must remain at or above 80% (monitored via scoverage).
- **Determinism**: Tests must rely solely on in-memory operations. No network, no wall clock, and no reliance on execution order.
- **Properties**: Invariants (e.g., commutativity of state merges, idempotency of reports) are validated using property-based testing via `munit.ScalaCheckSuite`.
- **Errors**: Negative testing is mandatory for all `DataQualityException` scenarios, asserting both the code and the message content.

## Layout

- `src/test/scala/`: Contains test suites. Each domain component (`Analyzer`, `VerificationSuite`, `CsvSource`, etc.) has its own `*Suite.scala` class.
- `src/test/resources/fixtures/`: Contains small CSV fixtures for parser validation and regression testing.
