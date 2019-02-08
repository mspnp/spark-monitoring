package org.apache.spark.metrics

import org.mockito.ArgumentMatcher

import scala.reflect.ClassTag

object TestImplicits {
  import scala.language.implicitConversions

  implicit def matcher[T](f: (T) => Boolean): ArgumentMatcher[T] =
    new ArgumentMatcher[T]() {
      def matches(o: Any): Boolean = f(o.asInstanceOf[T])
    }
}

object TestUtils {
  def getField[T: ClassTag](fieldName: String): java.lang.reflect.Field = {
    val field = scala.reflect.classTag[T].runtimeClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }
}
