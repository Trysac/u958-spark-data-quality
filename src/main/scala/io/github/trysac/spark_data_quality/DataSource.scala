package io.github.trysac.spark_data_quality

import java.nio.charset.StandardCharsets
import java.util.Locale
import scala.annotation.tailrec

/** Partitioned tabular dataset source.
  *
  * Mirrors distributed partitioned engines (such as Apache Spark's `mapPartitions`) where each
  * partition is scanned in a single pass to produce local analyzer states that are then merged.
  */
trait DataSource {

  /** The schema of this dataset. */
  def schema: Schema

  /** The partitions of rows in this dataset. */
  def partitions: Seq[Iterator[IndexedSeq[Cell]]]
}

/** An in-memory partitioned [[DataSource]].
  *
  * @param schema
  *   dataset schema
  * @param rows
  *   all rows of the dataset
  * @param partitionCount
  *   number of partitions to split `rows` across (at least 1)
  * @param emptyPartitionIndices
  *   optional 0-based indices where explicit empty partitions are inserted
  */
final class InMemorySource(
    val schema: Schema,
    val rows: IndexedSeq[IndexedSeq[Cell]],
    val partitionCount: Int = 1,
    val emptyPartitionIndices: Set[Int] = Set.empty,
) extends DataSource {

  if (partitionCount < 1) {
    throw DataQualityException(
      "invalid_partition_count",
      s"partitionCount must be >= 1, got $partitionCount",
    )
  }

  /** Partitions of rows in this dataset. */
  override def partitions: Seq[Iterator[IndexedSeq[Cell]]] = {
    val total = rows.length
    val baseSlices: Vector[IndexedSeq[IndexedSeq[Cell]]] =
      if (partitionCount == 1) {
        Vector(rows)
      } else {
        (0 until partitionCount).toVector.map { p =>
          val start = ((p.toLong * total) / partitionCount).toInt
          val end = (((p.toLong + 1L) * total) / partitionCount).toInt
          rows.slice(start, end)
        }
      }
    if (emptyPartitionIndices.isEmpty) {
      baseSlices.map(_.iterator)
    } else {
      val totalPartitions = partitionCount + emptyPartitionIndices.size
      @tailrec
      def buildPartitions(
          idx: Int,
          sliceIdx: Int,
          acc: Vector[Iterator[IndexedSeq[Cell]]],
      ): Vector[Iterator[IndexedSeq[Cell]]] = {
        if (idx < totalPartitions) {
          if (emptyPartitionIndices.contains(idx)) {
            buildPartitions(idx + 1, sliceIdx, acc :+ Iterator.empty)
          } else if (sliceIdx < baseSlices.length) {
            buildPartitions(idx + 1, sliceIdx + 1, acc :+ baseSlices(sliceIdx).iterator)
          } else {
            buildPartitions(idx + 1, sliceIdx, acc :+ Iterator.empty)
          }
        } else if (sliceIdx < baseSlices.length) {
          buildPartitions(idx, sliceIdx + 1, acc :+ baseSlices(sliceIdx).iterator)
        } else {
          acc
        }
      }
      buildPartitions(0, 0, Vector.empty)
    }
  }
}

/** Companion object for [[InMemorySource]]. */
object InMemorySource {

  /** Creates an [[InMemorySource]] over `rows` split across `partitionCount` partitions. */
  def apply(
      schema: Schema,
      rows: IndexedSeq[IndexedSeq[Cell]],
      partitionCount: Int = 1,
  ): InMemorySource =
    new InMemorySource(schema, rows, partitionCount)
}

/** RFC 4180 CSV parser producing a [[DataSource]]. */
object CsvSource {

  /** Default maximum CSV input size in bytes (8 MiB). */
  val DefaultMaxBytes: Int = 8_388_608

  /** Parses an RFC 4180 CSV string with a required header row matching `schema`.
    *
    * Empty unquoted or quoted fields (`""`) become [[Cell.Null]].
    *
    * @param text
    *   CSV text content
    * @param schema
    *   expected schema
    * @param maxBytes
    *   maximum UTF-8 byte length allowed (default 8_388_608)
    */
  def fromString(
      text: String,
      schema: Schema,
      maxBytes: Int = DefaultMaxBytes,
  ): DataSource = {
    val utf8Length = text.getBytes(StandardCharsets.UTF_8).length
    if (utf8Length > maxBytes) {
      throw DataQualityException(
        "input_too_large",
        s"CSV byte length $utf8Length exceeds maxBytes $maxBytes",
      )
    }

    val rawRecords = parseRecords(text)
    if (rawRecords.isEmpty) {
      throw DataQualityException("csv_syntax", "CSV input is empty (missing header row)", Some(1))
    }

    val (headerLine, headerFields) = rawRecords.head
    val expectedNames = schema.columns.map(_._1)
    if (headerFields != expectedNames) {
      throw DataQualityException(
        "csv_syntax",
        s"CSV header at line $headerLine does not match schema columns",
        Some(headerLine),
      )
    }

    val colCount = schema.columns.length
    val parsedRows = rawRecords.tail.map { case (lineNo, fields) =>
      if (fields.length != colCount) {
        throw DataQualityException(
          "csv_syntax",
          s"CSV row at line $lineNo has ${fields.length} fields, expected $colCount",
          Some(lineNo),
        )
      }
      fields.zipWithIndex.map { case (rawValue, colIdx) =>
        val (colName, colType) = schema.columns(colIdx)
        parseCell(rawValue, colName, colType, lineNo)
      }
    }

    InMemorySource(schema, parsedRows)
  }

  private def parseCell(
      raw: String,
      colName: String,
      colType: ColumnType,
      lineNo: Int,
  ): Cell = {
    if (raw.isEmpty) {
      Cell.Null
    } else {
      colType match {
        case ColumnType.Text =>
          Cell.Text(raw)
        case ColumnType.Int64 =>
          try {
            Cell.Int64(java.lang.Long.parseLong(raw))
          } catch {
            case _: NumberFormatException =>
              throw DataQualityException(
                "type_mismatch",
                s"Invalid Int64 value '${DataQualityException.sanitizeSnippet(raw)}' for column '${DataQualityException.sanitizeSnippet(colName)}' at line $lineNo",
                Some(lineNo),
              )
          }
        case ColumnType.Float64 =>
          try {
            Cell.Float64(java.lang.Double.parseDouble(raw))
          } catch {
            case _: NumberFormatException =>
              throw DataQualityException(
                "type_mismatch",
                s"Invalid Float64 value '${DataQualityException.sanitizeSnippet(raw)}' for column '${DataQualityException.sanitizeSnippet(colName)}' at line $lineNo",
                Some(lineNo),
              )
          }
        case ColumnType.Bool =>
          raw.toLowerCase(Locale.ROOT) match {
            case "true"  => Cell.Bool(true)
            case "false" => Cell.Bool(false)
            case _       =>
              throw DataQualityException(
                "type_mismatch",
                s"Invalid Bool value '${DataQualityException.sanitizeSnippet(raw)}' for column '${DataQualityException.sanitizeSnippet(colName)}' at line $lineNo",
                Some(lineNo),
              )
          }
      }
    }
  }

  private def parseRecords(text: String): Vector[(Int, Vector[String])] = {
    val len = text.length
    if (len == 0) {
      Vector.empty
    } else {
      @tailrec
      def loop(
          i: Int,
          line: Int,
          recordStartLine: Int,
          inQuotes: Boolean,
          quoteStartLine: Int,
          fieldJustClosedQuote: Boolean,
          atFieldStart: Boolean,
          currentField: StringBuilder,
          currentFields: Vector[String],
          records: Vector[(Int, Vector[String])],
      ): Vector[(Int, Vector[String])] = {
        if (i >= len) {
          if (inQuotes) {
            throw DataQualityException(
              "csv_syntax",
              s"Unclosed quoted field starting at line $quoteStartLine",
              Some(quoteStartLine),
            )
          }
          if (currentFields.nonEmpty || currentField.nonEmpty || fieldJustClosedQuote) {
            records :+ ((recordStartLine, currentFields :+ currentField.toString))
          } else {
            records
          }
        } else {
          val ch = text.charAt(i)
          if (inQuotes) {
            if (ch == '"') {
              if (i + 1 < len && text.charAt(i + 1) == '"') {
                currentField.append('"')
                loop(
                  i + 2,
                  line,
                  recordStartLine,
                  inQuotes = true,
                  quoteStartLine,
                  fieldJustClosedQuote = false,
                  atFieldStart = false,
                  currentField,
                  currentFields,
                  records,
                )
              } else {
                loop(
                  i + 1,
                  line,
                  recordStartLine,
                  inQuotes = false,
                  quoteStartLine,
                  fieldJustClosedQuote = true,
                  atFieldStart = false,
                  currentField,
                  currentFields,
                  records,
                )
              }
            } else if (ch == '\r') {
              val _ = currentField.append(ch)
              val advance = if (i + 1 < len && text.charAt(i + 1) == '\n') {
                val _ = currentField.append('\n')
                2
              } else {
                1
              }
              loop(
                i + advance,
                line + 1,
                recordStartLine,
                inQuotes = true,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = false,
                currentField,
                currentFields,
                records,
              )
            } else if (ch == '\n') {
              val _ = currentField.append(ch)
              loop(
                i + 1,
                line + 1,
                recordStartLine,
                inQuotes = true,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = false,
                currentField,
                currentFields,
                records,
              )
            } else {
              val _ = currentField.append(ch)
              loop(
                i + 1,
                line,
                recordStartLine,
                inQuotes = true,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = false,
                currentField,
                currentFields,
                records,
              )
            }
          } else {
            if (fieldJustClosedQuote) {
              if (ch == ',') {
                loop(
                  i + 1,
                  line,
                  recordStartLine,
                  inQuotes = false,
                  quoteStartLine,
                  fieldJustClosedQuote = false,
                  atFieldStart = true,
                  new StringBuilder,
                  currentFields :+ currentField.toString,
                  records,
                )
              } else if (ch == '\r') {
                val advance = if (i + 1 < len && text.charAt(i + 1) == '\n') 2 else 1
                val newRecords =
                  records :+ ((recordStartLine, currentFields :+ currentField.toString))
                loop(
                  i + advance,
                  line + 1,
                  line + 1,
                  inQuotes = false,
                  quoteStartLine,
                  fieldJustClosedQuote = false,
                  atFieldStart = true,
                  new StringBuilder,
                  Vector.empty,
                  newRecords,
                )
              } else if (ch == '\n') {
                val newRecords =
                  records :+ ((recordStartLine, currentFields :+ currentField.toString))
                loop(
                  i + 1,
                  line + 1,
                  line + 1,
                  inQuotes = false,
                  quoteStartLine,
                  fieldJustClosedQuote = false,
                  atFieldStart = true,
                  new StringBuilder,
                  Vector.empty,
                  newRecords,
                )
              } else {
                throw DataQualityException(
                  "csv_syntax",
                  s"Unexpected character after closing quote at line $line",
                  Some(line),
                )
              }
            } else if (atFieldStart && ch == '"') {
              loop(
                i + 1,
                line,
                recordStartLine,
                inQuotes = true,
                quoteStartLine = line,
                fieldJustClosedQuote = false,
                atFieldStart = false,
                currentField,
                currentFields,
                records,
              )
            } else if (ch == ',') {
              loop(
                i + 1,
                line,
                recordStartLine,
                inQuotes = false,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = true,
                new StringBuilder,
                currentFields :+ currentField.toString,
                records,
              )
            } else if (ch == '\r') {
              val advance = if (i + 1 < len && text.charAt(i + 1) == '\n') 2 else 1
              val newRecords =
                records :+ ((recordStartLine, currentFields :+ currentField.toString))
              loop(
                i + advance,
                line + 1,
                line + 1,
                inQuotes = false,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = true,
                new StringBuilder,
                Vector.empty,
                newRecords,
              )
            } else if (ch == '\n') {
              val newRecords =
                records :+ ((recordStartLine, currentFields :+ currentField.toString))
              loop(
                i + 1,
                line + 1,
                line + 1,
                inQuotes = false,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = true,
                new StringBuilder,
                Vector.empty,
                newRecords,
              )
            } else if (ch == '"') {
              throw DataQualityException(
                "csv_syntax",
                s"Unescaped quote in unquoted field at line $line",
                Some(line),
              )
            } else {
              val _ = currentField.append(ch)
              loop(
                i + 1,
                line,
                recordStartLine,
                inQuotes = false,
                quoteStartLine,
                fieldJustClosedQuote = false,
                atFieldStart = false,
                currentField,
                currentFields,
                records,
              )
            }
          }
        }
      }

      loop(
        i = 0,
        line = 1,
        recordStartLine = 1,
        inQuotes = false,
        quoteStartLine = 1,
        fieldJustClosedQuote = false,
        atFieldStart = true,
        currentField = new StringBuilder,
        currentFields = Vector.empty,
        records = Vector.empty,
      )
    }
  }
}
