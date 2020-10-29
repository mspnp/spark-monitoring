package org.apache.spark.metrics

import org.mockito.ArgumentMatcher

import scala.reflect.ClassTag
import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox
import scala.util.{Properties, Try}

object TestImplicits {

  import scala.language.implicitConversions

  implicit def matcher[T](f: (T) => Boolean): ArgumentMatcher[T] = new ArgumentMatcher[T] {
    override def matches(argument: T): Boolean = f(argument)
  }
}

object TestUtils {
  def getField[T: ClassTag](fieldName: String): java.lang.reflect.Field = {
    val field = scala.reflect.classTag[T].runtimeClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }

  def conditionalCode(spark2ver: String, spark3ver: String): Any = {
    val toolbox = currentMirror.mkToolBox()
    import toolbox.{eval, parse}
    if (Properties.versionString.stripPrefix("version ").startsWith("2.11"))
      eval(parse(spark2ver))
    else
      eval(parse(spark3ver))
  }

  def loadOneOf(clazzPaths: String*) =
    clazzPaths
      .toStream
      .map(cl => Try {
        Class.forName(cl)
      })
      .find(_.isSuccess)
      .get
}
