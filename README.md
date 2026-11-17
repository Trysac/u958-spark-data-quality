# spark-data-quality

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![CI](https://github.com/Trysac/u958-spark-data-quality/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u958-spark-data-quality/actions/workflows/ci.yml)

Deequ-style data quality checks with mergeable per-partition analyzer states, metric-history anomaly detection and JSON or Markdown verification reports

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

Maven coordinates `io.github.trysac:spark-data-quality_3` (Scala 3). Until it is published to Maven
Central, build it from a release tag through JitPack (`https://jitpack.io`), then declare in
`build.sbt`:

```scala
libraryDependencies += "io.github.trysac" %% "spark-data-quality" % "0.1.0"
```

Requires Scala 3.9 or newer and Java 21 or newer. No dependencies outside the Scala standard
library.

## Usage

```scala
import io.github.trysac.spark_data_quality.Text

// normalize trims surrounding whitespace and lower-cases the text.
val name = Text.normalize("  Hello World ") // "hello world"

// Blank input is rejected explicitly instead of returning an empty string.
Text.normalize("   ") // throws BlankInputException ("input is blank")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
