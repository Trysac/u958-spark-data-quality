package io.github.trysac.spark_data_quality

import org.scalacheck.Gen
import org.scalacheck.Prop.forAll

/** ScalaCheck property tests verifying state merge associativity, commutativity, and single-pass
  * equivalence across random partition splits for all 8 analyzers.
  */
class AnalyzerPropertiesSuite extends munit.ScalaCheckSuite {

  override def scalaCheckInitialSeed: String =
    org.scalacheck.rng.Seed(958L).toBase64

  override def scalaCheckTestParameters: org.scalacheck.Test.Parameters =
    super.scalaCheckTestParameters
      .withMinSuccessfulTests(500)

  private val testSchema: Schema = Schema(
    Vector(
      "id" -> ColumnType.Text,
      "num" -> ColumnType.Int64,
      "flt" -> ColumnType.Float64,
      "country" -> ColumnType.Text,
    ),
  )

  private val rowGen: Gen[Vector[Cell]] = for {
    idOpt <- Gen.option(Gen.oneOf("ORD-001", "ORD-002", "ORD-003", "BAD-1", "ORD-999"))
    numOpt <- Gen.option(Gen.choose(-1000L, 1000L))
    fltChoice <- Gen.choose(0, 10)
    fltVal <- Gen.choose(-500.0, 500.0)
    countryOpt <- Gen.option(Gen.oneOf("ES", "FR", "DE", "IT", "PT", "US", "GB"))
  } yield {
    val idCell = idOpt.fold(Cell.Null)(Cell.Text(_))
    val numCell = numOpt.fold(Cell.Null)(Cell.Int64(_))
    val fltCell =
      if (fltChoice == 0) Cell.Null
      else if (fltChoice == 1) Cell.Float64(Double.NaN)
      else Cell.Float64(fltVal)
    val countryCell = countryOpt.fold(Cell.Null)(Cell.Text(_))
    Vector(idCell, numCell, fltCell, countryCell)
  }

  private val datasetAndSplitsGen: Gen[(Vector[Vector[Cell]], Int, Int)] = for {
    rows <- Gen.listOfN(24, rowGen).map(_.toVector)
    split1 <- Gen.choose(0, 24)
    split2 <- Gen.choose(0, 24)
  } yield {
    val s1 = math.min(split1, split2)
    val s2 = math.max(split1, split2)
    (rows, s1, s2)
  }

  private val allAnalyzers: Vector[Analyzer] = Vector(
    SizeAnalyzer,
    CompletenessAnalyzer("id"),
    UniquenessAnalyzer("id"),
    MinAnalyzer("flt"),
    MaxAnalyzer("flt"),
    MeanAnalyzer("num"),
    PatternMatchAnalyzer("id", "^ORD-[0-9]{3}$"),
    IsContainedInAnalyzer("country", Set("ES", "FR", "DE", "IT", "PT")),
  )

  property(
    "merging states of any split of random rows equals the state computed in one pass for every analyzer",
  ) {
    forAll(datasetAndSplitsGen) { case (rows, s1, s2) =>
      val part1 = rows.slice(0, s1)
      val part2 = rows.slice(s1, s2)
      val part3 = rows.slice(s2, rows.length)

      allAnalyzers.forall { analyzer =>
        val onePass = analyzer.computePartition(rows, testSchema)
        val st1 = analyzer.computePartition(part1, testSchema)
        val st2 = analyzer.computePartition(part2, testSchema)
        val st3 = analyzer.computePartition(part3, testSchema)

        // Associativity: (st1 + st2) + st3 == st1 + (st2 + st3)
        val mergedLeft = analyzer.mergeUntyped(analyzer.mergeUntyped(st1, st2), st3)
        val mergedRight = analyzer.mergeUntyped(st1, analyzer.mergeUntyped(st2, st3))
        // Commutativity: st1 + st2 == st2 + st1
        val comm12 = analyzer.mergeUntyped(st1, st2)
        val comm21 = analyzer.mergeUntyped(st2, st1)

        mergedLeft == onePass && mergedRight == onePass && comm12 == comm21
      }
    }
  }
}
