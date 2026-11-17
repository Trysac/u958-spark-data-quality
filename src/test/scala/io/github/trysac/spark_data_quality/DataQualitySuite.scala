package io.github.trysac.spark_data_quality

/** Acceptance, edge-case, and error tests for `spark-data-quality`. */
class DataQualitySuite extends munit.FunSuite {

  private val ordersCheck: Check = Check(Level.Error, "orders")
    .hasCompleteness("email", Assertion.gte(0.999))
    .isUnique("order_id")
    .hasMin("amount", Assertion.gte(0.0))
    .isContainedIn("country", Set("ES", "FR", "DE", "IT", "PT"))

  test("acceptance 1: seeded 10_000 orders dataset (seed 958) detects all four anomalies") {
    val rows =
      SeededOrdersGenerator.generate(seed = 958L, totalRows = 10_000, injectAnomalies = true)
    val source = InMemorySource(SeededOrdersGenerator.OrdersSchema, rows, partitionCount = 1)

    val result = VerificationSuite.run(source, Seq(ordersCheck))

    assertEquals(result.status, "Error")
    assertEqualsDouble(result.metrics("completeness(email)"), 0.9963, 1e-9)
    assertEqualsDouble(result.metrics("uniqueness(order_id)"), 0.999, 1e-9)
    assertEqualsDouble(result.metrics("minimum(amount)"), -120.5, 1e-9)
    assertEqualsDouble(result.metrics("isContainedIn(country)"), 0.9988, 1e-9)
    assertEquals(result.constraintResults.length, 4)
    assert(result.constraintResults.forall(_.status == ConstraintStatus.Failure))
  }

  test("acceptance 2: dataset split into 1, 3 and 7 partitions gives identical metric values") {
    val rows =
      SeededOrdersGenerator.generate(seed = 958L, totalRows = 10_000, injectAnomalies = true)
    val fullCheck = Check(Level.Error, "orders")
      .hasSize(Assertion.gte(1.0))
      .hasCompleteness("email", Assertion.gte(0.999))
      .isUnique("order_id")
      .hasMin("amount", Assertion.gte(0.0))
      .hasMax("amount", Assertion.lte(1000.0))
      .hasMean("amount", Assertion.between(0.0, 1000.0))
      .hasPattern("order_id", "^ORD-[0-9]{6}$", Assertion.eq(1.0))
      .isContainedIn("country", Set("ES", "FR", "DE", "IT", "PT"))

    val r1 = VerificationSuite.run(
      InMemorySource(SeededOrdersGenerator.OrdersSchema, rows, partitionCount = 1),
      Seq(fullCheck),
    )
    val r3 = VerificationSuite.run(
      InMemorySource(SeededOrdersGenerator.OrdersSchema, rows, partitionCount = 3),
      Seq(fullCheck),
    )
    val r7 = VerificationSuite.run(
      InMemorySource(SeededOrdersGenerator.OrdersSchema, rows, partitionCount = 7),
      Seq(fullCheck),
    )

    assertEquals(r1.metrics.keySet, r3.metrics.keySet)
    assertEquals(r1.metrics.keySet, r7.metrics.keySet)
    for (k <- r1.metrics.keys) {
      assertEqualsDouble(r1.metrics(k), r3.metrics(k), 1e-9)
      assertEqualsDouble(r1.metrics(k), r7.metrics(k), 1e-9)
    }
  }

  test("acceptance 3: check status Warning vs Error vs Success") {
    val cleanRows =
      SeededOrdersGenerator.generate(seed = 958L, totalRows = 10_000, injectAnomalies = false)
    val cleanSource = InMemorySource(SeededOrdersGenerator.OrdersSchema, cleanRows)
    val cleanResult = VerificationSuite.run(cleanSource, Seq(ordersCheck))
    assertEquals(cleanResult.status, "Success")
    assert(cleanResult.constraintResults.forall(_.status == ConstraintStatus.Success))

    val seededRows =
      SeededOrdersGenerator.generate(seed = 958L, totalRows = 10_000, injectAnomalies = true)
    val seededSource = InMemorySource(SeededOrdersGenerator.OrdersSchema, seededRows)

    val warnCheck = Check(Level.Warning, "orders_warn")
      .hasCompleteness("email", Assertion.gte(0.999))
    val passErrorCheck = Check(Level.Error, "orders_size")
      .hasSize(Assertion.eq(10_000.0))

    val warnResult = VerificationSuite.run(seededSource, Seq(warnCheck, passErrorCheck))
    assertEquals(warnResult.status, "Warning")

    val errResult = VerificationSuite.run(seededSource, Seq(warnCheck, ordersCheck))
    assertEquals(errResult.status, "Error")
  }

  test("acceptance 4: Report.toJson and Report.toMarkdown formatting and byte stability") {
    val rows =
      SeededOrdersGenerator.generate(seed = 958L, totalRows = 10_000, injectAnomalies = true)
    val source = InMemorySource(SeededOrdersGenerator.OrdersSchema, rows)
    val result = VerificationSuite.run(source, Seq(ordersCheck))

    val json1 = Report.toJson(result)
    val json2 = Report.toJson(result)
    assertEquals(json1, json2)
    assert(
      json1.contains(
        "{\"check\":\"orders\",\"constraint\":\"completeness(email) >= 0.999\",\"status\":\"Failure\",\"value\":0.9963}",
      ),
      s"JSON did not contain expected constraint line:\n$json1",
    )

    val md1 = Report.toMarkdown(result)
    val md2 = Report.toMarkdown(result)
    assertEquals(md1, md2)
    assert(md1.startsWith("# Verification: Error\n"))
    assert(md1.contains("| orders | completeness(email) >= 0.999 | Failure | 0.9963 |"))
  }

  test("acceptance 5: MetricsRepository history with RelativeRateOfChange and ZScore") {
    val repo = new InMemoryMetricsRepository()
    repo.save(ResultKey(1_000L, Map("env" -> "prod")), Map("size" -> 10_000.0))
    repo.save(ResultKey(2_000L, Map("env" -> "prod")), Map("size" -> 10_100.0))
    repo.save(ResultKey(3_000L, Map("env" -> "prod")), Map("size" -> 9_950.0))

    val schema = Schema(Vector("id" -> ColumnType.Int64))
    val rows4000 = Vector.tabulate(4_000)(i => Vector(Cell.Int64(i.toLong)))
    val rows9900 = Vector.tabulate(9_900)(i => Vector(Cell.Int64(i.toLong)))

    val rroc = AnomalyCheck(
      "size",
      RelativeRateOfChange(maxDecrease = 0.5, maxIncrease = 2.0),
      Level.Error,
    )

    val resAnomaly = VerificationSuite.run(
      InMemorySource(schema, rows4000),
      checks = Nil,
      repository = Some(repo),
      key = Some(ResultKey(4_000L)),
      anomalyChecks = Seq(rroc),
    )
    assertEquals(resAnomaly.status, "Error")
    assertEquals(resAnomaly.constraintResults.head.status, ConstraintStatus.Failure)

    // Check at 3_500L (before 4_000L, so latest history is 3_000L -> 9_950)
    val resAccepted = VerificationSuite.run(
      InMemorySource(schema, rows9900),
      checks = Nil,
      repository = Some(repo),
      key = Some(ResultKey(3_500L)),
      anomalyChecks = Seq(rroc),
    )
    assertEquals(resAccepted.status, "Success")
    assertEquals(resAccepted.constraintResults.head.status, ConstraintStatus.Success)

    // ZScore with >= 3 points succeeds for normal value and flags outlier
    val zCheck = AnomalyCheck("size", ZScore(maxDeviations = 3.0), Level.Error)
    val resZNormal = VerificationSuite.run(
      InMemorySource(schema, rows9900),
      checks = Nil,
      repository = Some(repo),
      key = Some(ResultKey(3_600L)),
      anomalyChecks = Seq(zCheck),
    )
    assertEquals(resZNormal.status, "Success")

    val resZOutlier = VerificationSuite.run(
      InMemorySource(schema, rows4000),
      checks = Nil,
      repository = Some(repo),
      key = Some(ResultKey(3_700L)),
      anomalyChecks = Seq(zCheck),
    )
    assertEquals(resZOutlier.status, "Error")

    // ZScore with fewer than 3 history points raises insufficient_history
    val shortRepo = new InMemoryMetricsRepository()
    shortRepo.save(ResultKey(100L), Map("size" -> 10_000.0))
    shortRepo.save(ResultKey(200L), Map("size" -> 10_100.0))

    val err = intercept[DataQualityException] {
      VerificationSuite.run(
        InMemorySource(schema, rows9900),
        checks = Nil,
        repository = Some(shortRepo),
        key = Some(ResultKey(300L)),
        anomalyChecks = Seq(zCheck),
      )
    }
    assertEquals(err.code, "insufficient_history")
  }

  test(
    "acceptance 6: CsvSource RFC 4180 parsing, CRLF, size limit, unclosed quote, type mismatch",
  ) {
    val schema = Schema(
      Vector(
        "id" -> ColumnType.Int64,
        "name" -> ColumnType.Text,
        "note" -> ColumnType.Text,
        "score" -> ColumnType.Float64,
        "active" -> ColumnType.Bool,
      ),
    )
    val csvText =
      "id,name,note,score,active\r\n" +
        "1,\"Alice, Jr.\",\"She said \"\"hi\"\"\",98.5,true\r\n" +
        "2,Bob,,10.0,false\r\n" +
        "3,\"\",\"line1\r\nline2\",0.0,TRUE\r\n"

    val source = CsvSource.fromString(csvText, schema)
    val rows = source.partitions.flatMap(_.toVector).toVector
    assertEquals(rows.length, 3)
    assertEquals(
      rows(0),
      Vector(
        Cell.Int64(1L),
        Cell.Text("Alice, Jr."),
        Cell.Text("She said \"hi\""),
        Cell.Float64(98.5),
        Cell.Bool(true),
      ),
    )
    assertEquals(
      rows(1),
      Vector(
        Cell.Int64(2L),
        Cell.Text("Bob"),
        Cell.Null,
        Cell.Float64(10.0),
        Cell.Bool(false),
      ),
    )
    assertEquals(
      rows(2),
      Vector(
        Cell.Int64(3L),
        Cell.Null,
        Cell.Text("line1\r\nline2"),
        Cell.Float64(0.0),
        Cell.Bool(true),
      ),
    )

    // 8_388_609 bytes raises input_too_large
    val oversized = "a" * 8_388_609
    val sizeErr = intercept[DataQualityException] {
      CsvSource.fromString(oversized, Schema(Vector("a" -> ColumnType.Text)))
    }
    assertEquals(sizeErr.code, "input_too_large")

    // Unclosed quote raises csv_syntax with its line number
    val unclosedCsv = "id,name\n1,ok\n2,\"unclosed\n3,after"
    val syntaxErr = intercept[DataQualityException] {
      CsvSource.fromString(
        unclosedCsv,
        Schema(Vector("id" -> ColumnType.Int64, "name" -> ColumnType.Text)),
      )
    }
    assertEquals(syntaxErr.code, "csv_syntax")
    assertEquals(syntaxErr.line, Some(3))

    // "abc" in a long column raises type_mismatch
    val badLongCsv = "id,name\nabc,Alice\n"
    val typeErr = intercept[DataQualityException] {
      CsvSource.fromString(
        badLongCsv,
        Schema(Vector("id" -> ColumnType.Int64, "name" -> ColumnType.Text)),
      )
    }
    assertEquals(typeErr.code, "type_mismatch")
  }

  test(
    "acceptance 7: typed error codes for unknown_column, type_mismatch, invalid_regex, state_too_large, repository_full",
  ) {
    val schema = Schema(
      Vector(
        "name" -> ColumnType.Text,
        "age" -> ColumnType.Int64,
      ),
    )
    val rows = Vector(Vector(Cell.Text("Alice"), Cell.Int64(30L)))
    val source = InMemorySource(schema, rows)

    // Check on column "missing" gives 'unknown_column'
    val unknownColErr = intercept[DataQualityException] {
      VerificationSuite.run(source, Seq(Check(Level.Error, "c").isComplete("missing")))
    }
    assertEquals(unknownColErr.code, "unknown_column")

    // Mean on a string column gives 'type_mismatch'
    val meanTypeErr = intercept[DataQualityException] {
      VerificationSuite.run(
        source,
        Seq(Check(Level.Error, "c").hasMean("name", Assertion.gte(0.0))),
      )
    }
    assertEquals(meanTypeErr.code, "type_mismatch")

    // Regex "(" gives 'invalid_regex'
    val regexErr = intercept[DataQualityException] {
      Check(Level.Error, "c").hasPattern("name", "(")
    }
    assertEquals(regexErr.code, "invalid_regex")

    // Uniqueness state above 1_000_000 distinct values gives 'state_too_large'
    val half = 500_001
    val mapA = (0 until half).map(i => (Cell.Int64(i.toLong): Cell) -> 1L).toMap
    val mapB = (half until (half * 2)).map(i => (Cell.Int64(i.toLong): Cell) -> 1L).toMap
    val stateA = UniquenessState(mapA, half.toLong)
    val stateB = UniquenessState(mapB, half.toLong)
    val stateErr = intercept[DataQualityException] {
      UniquenessAnalyzer("age").merge(stateA, stateB)
    }
    assertEquals(stateErr.code, "state_too_large")

    // Also verify computePartition directly hits state_too_large when cap is exceeded
    val smallCapAnalyzer = UniquenessAnalyzer("age", maxDistinctValues = 3)
    val fourRows = (1L to 4L).toVector.map(i => Vector(Cell.Text("x"), Cell.Int64(i)))
    val partStateErr = intercept[DataQualityException] {
      smallCapAnalyzer.computePartition(fourRows, schema)
    }
    assertEquals(partStateErr.code, "state_too_large")

    // Repository entry 10_001 gives 'repository_full'
    val repo = new InMemoryMetricsRepository()
    for (i <- 1 to 10_000) {
      repo.save(ResultKey(i.toLong), Map("size" -> i.toDouble))
    }
    assertEquals(repo.size, 10_000)
    val repoFullErr = intercept[DataQualityException] {
      repo.save(ResultKey(10_001L), Map("size" -> 10_001.0))
    }
    assertEquals(repoFullErr.code, "repository_full")
  }

  test(
    "edge cases: empty partition among non-empty ones, all-null column, NaN in Float64, prefix regex",
  ) {
    val schema = Schema(
      Vector(
        "all_null" -> ColumnType.Text,
        "score" -> ColumnType.Float64,
        "code" -> ColumnType.Text,
      ),
    )
    val rows = Vector(
      Vector(Cell.Null, Cell.Float64(10.0), Cell.Text("ABC-123")),
      Vector(Cell.Null, Cell.Float64(Double.NaN), Cell.Text("ABC-123-EXTRA")),
      Vector(Cell.Null, Cell.Float64(20.0), Cell.Text("ABC-456")),
    )

    // Source with empty partitions mixed with non-empty partitions
    val source = new InMemorySource(
      schema,
      rows,
      partitionCount = 2,
      emptyPartitionIndices = Set(0, 2),
    )
    assertEquals(source.partitions.length, 4)

    // 1. All-null column gives completeness 0.0
    // 2. NaN in Float64 counts as non-null for completeness (3/3 = 1.0), excluded from min/max/mean (min=10, max=20, mean=15)
    // 3. Regex matching only a prefix ("ABC-[0-9]{3}") does NOT match "ABC-123-EXTRA" (2/3 match)
    val check = Check(Level.Warning, "edges")
      .hasCompleteness("all_null", Assertion.eq(0.0))
      .isComplete("score")
      .hasMin("score", Assertion.eq(10.0))
      .hasMax("score", Assertion.eq(20.0))
      .hasMean("score", Assertion.eq(15.0))
      .hasPattern("code", "ABC-[0-9]{3}", Assertion.between(0.66, 0.67))

    val res = VerificationSuite.run(source, Seq(check))
    assertEquals(res.status, "Success")
    assertEqualsDouble(res.metrics("completeness(all_null)"), 0.0, 1e-9)
    assertEqualsDouble(res.metrics("completeness(score)"), 1.0, 1e-9)
    assertEqualsDouble(res.metrics("minimum(score)"), 10.0, 1e-9)
    assertEqualsDouble(res.metrics("maximum(score)"), 20.0, 1e-9)
    assertEqualsDouble(res.metrics("mean(score)"), 15.0, 1e-9)
    assertEqualsDouble(res.metrics("patternMatch(code)"), 2.0 / 3.0, 1e-9)

    // Evaluating uniqueness on an entirely null column raises 'empty_dataset'
    val uniqErr = intercept[DataQualityException] {
      VerificationSuite.run(source, Seq(Check(Level.Error, "u").isUnique("all_null")))
    }
    assertEquals(uniqErr.code, "empty_dataset")
  }

  test(
    "additional coverage: CSV syntax errors, schema validation, snippet truncation, and helpers",
  ) {
    val schema = Schema(
      Vector(
        "i" -> ColumnType.Int64,
        "f" -> ColumnType.Float64,
        "b" -> ColumnType.Bool,
        "t" -> ColumnType.Text,
      ),
    )

    // Empty CSV
    assertEquals(
      intercept[DataQualityException](CsvSource.fromString("", schema)).code,
      "csv_syntax",
    )
    // Wrong header
    assertEquals(
      intercept[DataQualityException](CsvSource.fromString("wrong,f,b,t\n", schema)).code,
      "csv_syntax",
    )
    // Wrong field count
    assertEquals(
      intercept[DataQualityException](CsvSource.fromString("i,f,b,t\n1,2.0\n", schema)).code,
      "csv_syntax",
    )
    // Invalid float
    assertEquals(
      intercept[DataQualityException](
        CsvSource.fromString("i,f,b,t\n1,not_float,true,hi\n", schema),
      ).code,
      "type_mismatch",
    )
    // Invalid bool
    assertEquals(
      intercept[DataQualityException](
        CsvSource.fromString("i,f,b,t\n1,2.0,maybe,hi\n", schema),
      ).code,
      "type_mismatch",
    )
    // Unexpected character after closing quote
    assertEquals(
      intercept[DataQualityException](
        CsvSource.fromString("i,f,b,t\n1,2.0,true,\"hi\"extra\n", schema),
      ).code,
      "csv_syntax",
    )
    // Unescaped quote inside unquoted field
    assertEquals(
      intercept[DataQualityException](
        CsvSource.fromString("i,f,b,t\n1,2.0,true,ab\"cd\n", schema),
      ).code,
      "csv_syntax",
    )

    // Long snippet truncation <= 200 chars of input
    val hugeCol = "x" * 500
    val colErr = intercept[DataQualityException](schema.resolve(hugeCol))
    assertEquals(colErr.code, "unknown_column")
    assert(!colErr.getMessage.contains(hugeCol))
    assert(colErr.toString.startsWith("DataQualityException(unknown_column):"))

    // Empty dataset ratio metrics raise empty_dataset
    val emptySource = InMemorySource(schema, Vector.empty)
    for (
      chk <- Seq(
        Check(Level.Error, "e").isComplete("t"),
        Check(Level.Error, "e").hasMin("f", Assertion.gte(0.0)),
        Check(Level.Error, "e").hasMax("f", Assertion.lte(1.0)),
        Check(Level.Error, "e").hasMean("f", Assertion.eq(0.0)),
        Check(Level.Error, "e").hasPattern("t", ".*"),
        Check(Level.Error, "e").isContainedIn("t", Set("a")),
      )
    ) {
      val err = intercept[DataQualityException](VerificationSuite.run(emptySource, Seq(chk)))
      assertEquals(err.code, "empty_dataset")
    }
  }
}
