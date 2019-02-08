package org.apache.spark.metrics

object Implicits {
  implicit class StringExtensions(val input: String) {
    def isNullOrEmpty: Boolean = input == null || input.trim.isEmpty
  }
}
