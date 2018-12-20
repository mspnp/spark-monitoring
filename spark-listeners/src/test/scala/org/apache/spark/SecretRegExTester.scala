package org.apache.spark

import org.scalatest.FunSuite

// This class is place holder to remind that test cases are needed for all main folder classes

class SecretRegExTester extends FunSuite {

  case class RegExScopeKey(scope: String, key: String)

  test("reg ex defined for extracting loganalytics should match ") {

    val secretScopeAndKeyValidation = "^([a-zA-Z0-9_\\.-]{1,128})\\:([a-zA-Z0-9_\\.-]{1,128})$"
      .r("scope", "key")

    val someKey = "someKey1_.-"
    val someScope = "someScope1_.-"
    val splitter = ":"

    val scopeAndKey = someScope.concat(splitter).concat(someKey)
    val regExScopeKey = secretScopeAndKeyValidation.findFirstMatchIn(scopeAndKey) match {

      case Some(value) => RegExScopeKey(value.group("scope"), value.group("key"))
      case None => RegExScopeKey("notFound", "notFound")

    }
    assert(regExScopeKey.scope.equals(someScope))
    assert(regExScopeKey.key.equals(someKey))

  }

}
