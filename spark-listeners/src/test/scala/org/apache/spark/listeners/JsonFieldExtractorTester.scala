package org.apache.spark.listeners

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.scheduler.SparkListenerApplicationStart
import org.apache.spark.util.{JsonProtocol, Utils}
import org.apache.spark.{SparkFunSuite, TimeGenerated}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JNothing, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse}

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


  test("should insert a new field ") {

    val mockSparkListAppStartEvent = SparkListenerApplicationStart(
      "someAppName",
      Option.empty[String],
      1L,
      "someSparkUser",
      Option.empty[String]
    )

    val mockTimeGenerated = TimeGenerated("someTimeGenerated")
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val jValEvent = JsonProtocol.sparkEventToJson(mockSparkListAppStartEvent)
    val jsonStringEvent = compact(jValEvent)
    val expectedFinalEvent = jsonStringEvent.substring(0, jsonStringEvent.length - 1).concat(",\"TimeGenerated\":\"someTimeGenerated\"}")

    val jValTimeGenerated = parse(mapper.writeValueAsString(mockTimeGenerated))
    val finalValEvent = jValEvent.merge(jValTimeGenerated)

    val actualFinalEvent = compact(finalValEvent)

    println(expectedFinalEvent)
    println(actualFinalEvent)
    assert(expectedFinalEvent.contentEquals(actualFinalEvent))


  }


}
