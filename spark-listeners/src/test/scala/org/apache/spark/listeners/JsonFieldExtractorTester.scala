package org.apache.spark.listeners

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.jackson.JsonMethods.parse

class JsonFieldExtractorTester extends SparkFunSuite {


  test("should extract fields at top level ") {
    implicit val formats = DefaultFormats
    val testJsonString = "{ \"name\":\"John\", \"age\":30, \"car\":null }"
    val jsonFlatFieldName = "name"

    val fieldValue = Utils.jsonOption(
      extractJValueWrapper(parse(testJsonString)
        , jsonFlatFieldName)
    )

    assert(fieldValue.get.extract[String].contentEquals("John"))
  }

  test("should extract fields at any level") {

    implicit val formats = DefaultFormats
    val testJsonString = "{\"shipmentLocation\":{\"personName\":{\"first\":\"firstName\",\"generationalSuffix\":\"string\",\"last\":\"string\",\"middle\":\"string\",\"preferredFirst\":\"string\",\"prefix\":\"string\",\"professionalSuffixes\":[\"string\"]}},\"trackingNumber\":\"string\"}"
    val jsonFlatFieldName = "shipmentLocation.personName.first"

    val fieldValue = Utils.jsonOption(
      extractJValueWrapper(parse(testJsonString)
        , jsonFlatFieldName)
    )

    assert(fieldValue.get.extract[String].contentEquals("firstName"))
  }

  test("should get undefined when extract a field that doesnot exists") {
    implicit val formats = DefaultFormats
    val testJsonString = "{ \"name\":\"John\", \"age\":30, \"car\":null }"
    val jsonFlatFieldName = "fieldDoesnotExists"

    val fieldValue = Utils.jsonOption(
      extractJValueWrapper(parse(testJsonString)
        , jsonFlatFieldName)
    )

    assert(fieldValue.isEmpty)
  }


  def extractJValueWrapper(input: JValue, flatFieldName: String): JValue = {

    var jVal = input
    val fields = flatFieldName.split('.')

    for (f <- fields.indices) {
      if (jVal != JNothing) {
        jVal = extractJValue(jVal, fields(f))
      }
      else {
        return JNothing
      }
    }
    jVal
  }

  def extractJValue(input: JValue, field: String) = {

    input \ field
  }


}
