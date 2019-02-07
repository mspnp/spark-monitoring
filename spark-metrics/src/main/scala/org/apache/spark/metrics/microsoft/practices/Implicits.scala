package org.apache.spark.metrics.microsoft.practices

object Implicits {
  implicit class StringExtensions(val input: String) {
    def isNullOrEmpty: Boolean = input == null || input.trim.isEmpty
  }
}
