package io.github.trysac.spark_data_quality

/** A typed cell value in a tabular dataset row. */
enum Cell {

  /** A missing / null value. */
  case Null

  /** A UTF-8 string value. */
  case Text(value: String)

  /** A 64-bit signed integer value. */
  case Int64(value: Long)

  /** A 64-bit IEEE-754 floating-point value. */
  case Float64(value: Double)

  /** A boolean value. */
  case Bool(value: Boolean)

  /** Renders the non-null cell value as a canonical string for set membership checks. */
  def asString: Option[String] = this match {
    case Cell.Null           => None
    case Cell.Text(value)    => Some(value)
    case Cell.Int64(value)   => Some(value.toString)
    case Cell.Float64(value) => Some(value.toString)
    case Cell.Bool(value)    => Some(value.toString)
  }
}

/** Supported column types in a [[Schema]]. */
enum ColumnType {

  /** String column type. */
  case Text

  /** 64-bit integer column type. */
  case Int64

  /** 64-bit floating-point column type. */
  case Float64

  /** Boolean column type. */
  case Bool
}

/** Schema of a tabular [[DataSource]].
  *
  * @param columns
  *   ordered list of column names and their types
  */
final case class Schema(columns: Vector[(String, ColumnType)]) {

  private val indexByName: Map[String, (Int, ColumnType)] =
    columns.zipWithIndex.map { case ((name, colType), idx) => name -> (idx, colType) }.toMap

  /** Looks up the 0-based column index and type for `column`, or raises `unknown_column`. */
  def resolve(column: String): (Int, ColumnType) =
    indexByName.getOrElse(
      column,
      throw DataQualityException(
        "unknown_column",
        s"Unknown column '${DataQualityException.sanitizeSnippet(column)}'",
      ),
    )

  /** Verifies that `cell` conforms to `expected` column type, or raises `type_mismatch`. */
  def validateCell(column: String, expected: ColumnType, cell: Cell): Unit = {
    val valid = (expected, cell) match {
      case (_, Cell.Null)                        => true
      case (ColumnType.Text, Cell.Text(_))       => true
      case (ColumnType.Int64, Cell.Int64(_))     => true
      case (ColumnType.Float64, Cell.Float64(_)) => true
      case (ColumnType.Bool, Cell.Bool(_))       => true
      case _                                     => false
    }
    if (!valid) {
      throw DataQualityException(
        "type_mismatch",
        s"Column '${DataQualityException.sanitizeSnippet(column)}' expects $expected, got $cell",
      )
    }
  }
}
