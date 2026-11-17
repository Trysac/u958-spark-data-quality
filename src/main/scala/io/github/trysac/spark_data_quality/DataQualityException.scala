package io.github.trysac.spark_data_quality

/** Exception raised when data quality configuration, parsing, or evaluation fails.
  *
  * @param code
  *   stable machine-readable error code
  * @param message
  *   human-readable description holding at most 200 characters of untrusted input
  * @param line
  *   optional 1-based line number for syntax errors
  */
final class DataQualityException(
    val code: String,
    message: String,
    val line: Option[Int] = None,
) extends IllegalArgumentException(message) {

  /** Returns the machine-readable error code. */
  override def toString: String = s"DataQualityException($code): $getMessage"
}

/** Factory and helpers for [[DataQualityException]]. */
object DataQualityException {

  /** Maximum number of characters from untrusted input included in an error message. */
  val MaxInputSnippetLength: Int = 200

  /** Truncates an untrusted snippet to at most [[MaxInputSnippetLength]] characters. */
  def sanitizeSnippet(input: String): String = {
    if (input.length <= MaxInputSnippetLength) input
    else input.substring(0, MaxInputSnippetLength)
  }

  /** Creates a [[DataQualityException]] with the given code and message. */
  def apply(code: String, message: String, line: Option[Int] = None): DataQualityException =
    new DataQualityException(code, message, line)
}
