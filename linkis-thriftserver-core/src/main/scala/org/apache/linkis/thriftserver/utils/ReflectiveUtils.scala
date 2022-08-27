package org.apache.linkis.thriftserver.utils

import org.apache.commons.lang3.reflect.FieldUtils
import org.apache.linkis.common.utils.Utils

/**
 *
 * @date 2022-08-16
 * @author enjoyyin
 * @since 0.5.0
 */
object ReflectiveUtils {

  def readField[T](obj: Any, fieldName: String): T = FieldUtils.readDeclaredField(obj, fieldName, true) match {
    case value: T => value
    case null => null.asInstanceOf[T]
  }

  def readSuperField[T](obj: Any, fieldName: String): T = {
    val field = FieldUtils.getDeclaredField(obj.getClass.getSuperclass, fieldName, true)
    FieldUtils.readField(field, obj) match {
      case value: T => value
      case null => null.asInstanceOf[T]
    }
  }

  def writeField(obj: Any, fieldName: String, fieldValue: Any): Unit =
    FieldUtils.writeDeclaredField(obj, fieldName, fieldValue, true)

  def writeField(objClass: Class[_], obj: Any, fieldName: String, fieldValue: Any): Unit = {
    val field = FieldUtils.getDeclaredField(objClass, fieldName, true)
    FieldUtils.writeField(field, obj, fieldValue)
  }

  def writeSuperField(obj: Any, fieldName: String, fieldValue: Any): Unit ={
    val field = FieldUtils.getDeclaredField(obj.getClass.getSuperclass, fieldName, true)
    FieldUtils.writeField(field, obj, fieldValue)
  }

  def invokeMethod[T](objClass: Class[_], obj: Any, methodName: String, parameters: Array[Any]): T = {
    val parameterTypes: Array[Class[_]] = if(parameters != null && parameters.nonEmpty) parameters.map(_.getClass) else Array.empty
    val method = Utils.tryCatch(objClass.getDeclaredMethod(methodName, parameterTypes:_*)){
      case _: NoSuchMethodException =>
        objClass.getSuperclass.getDeclaredMethod(methodName, parameterTypes:_*)
    }
    method.setAccessible(true)
    val result = if(parameters == null || parameters.isEmpty) method.invoke(obj) else {
      val parameterArray = parameters.map {
        case obj: Object => obj
        case null => null
        case p if p.getClass.isPrimitive => p.asInstanceOf[Object]
      }
      method.invoke(obj, parameterArray:_*)
    }
    result match {
      case value: T => value
      case null => null.asInstanceOf[T]
    }
  }

  def invokeMethod[T](obj: Any, methodName: String, parameters: Array[Any]): T = invokeMethod(obj.getClass, obj, methodName, parameters)

  def invokeGetter[T](obj: Any, methodName: String): T = invokeMethod(obj, methodName, null)

  def invokeSetter[T](obj: Any, methodName: String, value: Any): Unit =
    invokeMethod(obj, methodName, Array(value))

  def invokeSetter[T](objClass: Class[_], obj: Any, methodName: String, value: Any): Unit =
    invokeMethod(objClass, obj, methodName, Array(value))

}
