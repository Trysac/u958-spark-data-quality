package io.github.trysac.spark_data_quality

/** Thrown when a text argument is empty or consists only of whitespace. */
final class BlankInputException extends IllegalArgumentException("input is blank")
