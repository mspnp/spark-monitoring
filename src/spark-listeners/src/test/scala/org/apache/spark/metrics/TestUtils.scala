package org.apache.spark.metrics

import org.mockito.ArgumentMatcher

import scala.reflect.ClassTag
import scala.util.Try

object TestImplicits {

  import scala.language.implicitConversions

  implicit def matcher[T](f: (T) => Boolean): ArgumentMatcher[T] = new ArgumentMatcher[T] {
    override def matches(argument: T): Boolean = f(argument)
  }

  implicit class ListImplicits[T](lst: List[T]) {
    def insertAt(index: Int, objs: T*): List[T] = {
      val (a, b) = lst.splitAt(index)
      a ::: objs.toList ::: b
    }

  }

}

object TestUtils {
  def getField[T: ClassTag](fieldName: String): java.lang.reflect.Field = {
    val field = scala.reflect.classTag[T].runtimeClass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }

  def newInstance[T](clazz: Class[T], args: Any*): Option[T] = {
    val argslst = args.map(_.asInstanceOf[Object])
    val res = clazz.getConstructors
      .toStream
      .filter(_.getParameterTypes.length == args.size)
    val res2 = res
      .map(c => Try {
        c.newInstance(argslst: _*)
      })
      .flatMap(_.toOption)
      .headOption
      .map(_.asInstanceOf[T])
    res2
  }

  def loadOneOf(clazzPaths: String*): Option[Class[_]] =
    clazzPaths
      .toStream
      .map(cl => Try {
        Class.forName(cl)
      })
      .find(_.isSuccess)
      .map(_.get)
}
