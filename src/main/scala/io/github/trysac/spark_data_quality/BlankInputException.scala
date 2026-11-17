package io.github.trysac.spark_data_quality

/** The input is empty or holds only whitespace; the message is `input is blank`. */
final class BlankInputException extends IllegalArgumentException("input is blank")
