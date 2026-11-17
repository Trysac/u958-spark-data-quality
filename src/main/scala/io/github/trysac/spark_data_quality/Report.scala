package io.github.trysac.spark_data_quality

import java.math.{BigDecimal as JBigDecimal, RoundingMode}

/** Deterministic JSON and Markdown report formatters for [[VerificationResult]].
  *
  * Metric values are rounded to 4 decimal places using `RoundingMode.HALF_EVEN`.
  */
object Report {

  /** Rounds `value` to 4 decimal places with `HALF_EVEN` and formats it as a minimal JSON number.
    */
  def formatNumber(value: Double): String = {
    val bd = JBigDecimal.valueOf(value).setScale(4, RoundingMode.HALF_EVEN).stripTrailingZeros()
    val plain = bd.toPlainString
    if (plain == "-0") "0" else plain
  }

  /** Renders `result` as a deterministic, byte-stable JSON document.
    *
    * Each constraint entry is formatted as:
    * `{"check":"...","constraint":"...","status":"...","value":...}` with keys in that exact order.
    */
  def toJson(result: VerificationResult): String = {
    val sb = new java.lang.StringBuilder
    val _ = sb.append("{\n")
    val _ = sb.append("  \"status\": \"").append(escapeJson(result.status)).append("\",\n")
    val _ = sb.append("  \"metrics\": {")
    val sortedMetrics = result.metrics.toVector.sortBy(_._1)
    if (sortedMetrics.nonEmpty) {
      val _ = sb.append("\n")
      for (i <- sortedMetrics.indices) {
        val (k, v) = sortedMetrics(i)
        val _ = sb.append("    \"").append(escapeJson(k)).append("\": ").append(formatNumber(v))
        if (i + 1 < sortedMetrics.length) {
          val _ = sb.append(",")
        }
        val _ = sb.append("\n")
      }
      val _ = sb.append("  ")
    }
    val _ = sb.append("},\n")
    val _ = sb.append("  \"constraints\": [")
    if (result.constraintResults.nonEmpty) {
      val _ = sb.append("\n")
      for (j <- result.constraintResults.indices) {
        val c = result.constraintResults(j)
        val _ = sb
          .append("    {\"check\":\"")
          .append(escapeJson(c.checkName))
          .append("\",\"constraint\":\"")
          .append(escapeJson(c.constraint))
          .append("\",\"status\":\"")
          .append(escapeJson(c.status.toString))
          .append("\",\"value\":")
          .append(formatNumber(c.value))
          .append("}")
        if (j + 1 < result.constraintResults.length) {
          val _ = sb.append(",")
        }
        val _ = sb.append("\n")
      }
      val _ = sb.append("  ")
    }
    val _ = sb.append("]\n}")
    sb.toString
  }

  /** Renders `result` as a deterministic, byte-stable Markdown report starting with
    * `# Verification: <status>`.
    */
  def toMarkdown(result: VerificationResult): String = {
    val sb = new java.lang.StringBuilder
    val _ = sb.append("# Verification: ").append(result.status).append("\n\n")
    val _ = sb.append("## Constraints\n\n")
    val _ = sb.append("| Check | Constraint | Status | Value |\n")
    val _ = sb.append("| --- | --- | --- | --- |\n")
    for (c <- result.constraintResults) {
      val _ = sb
        .append("| ")
        .append(c.checkName)
        .append(" | ")
        .append(c.constraint)
        .append(" | ")
        .append(c.status.toString)
        .append(" | ")
        .append(formatNumber(c.value))
        .append(" |\n")
    }
    val _ = sb.append("\n## Metrics\n\n")
    val _ = sb.append("| Metric | Value |\n")
    val _ = sb.append("| --- | --- |\n")
    for ((k, v) <- result.metrics.toVector.sortBy(_._1)) {
      val _ = sb.append("| ").append(k).append(" | ").append(formatNumber(v)).append(" |\n")
    }
    sb.toString
  }

  private def escapeJson(s: String): String = {
    val sb = new java.lang.StringBuilder(s.length)
    for (i <- 0 until s.length) {
      s.charAt(i) match {
        case '"'          => val _ = sb.append("\\\"")
        case '\\'         => val _ = sb.append("\\\\")
        case '\b'         => val _ = sb.append("\\b")
        case '\f'         => val _ = sb.append("\\f")
        case '\n'         => val _ = sb.append("\\n")
        case '\r'         => val _ = sb.append("\\r")
        case '\t'         => val _ = sb.append("\\t")
        case c if c < ' ' => val _ = sb.append(f"\\u${c.toInt}%04x")
        case c            => val _ = sb.append(c)
      }
    }
    sb.toString
  }
}
