package org.apache.linkis.thriftserver.exception

import org.apache.linkis.common.exception.{ErrorException, WarnException}

/**
 *
 * @date 2022-08-16
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisThriftServerNotSupportException(errCode: Int, desc: String) extends ErrorException(errCode, desc) {
  def this(methodName: String) = this(80001, s"Method $methodName is not supported!")
}
class LinkisThriftServerWarnException(errorCode: Int, desc: String, cause: Throwable) extends WarnException(errorCode, desc) {
  if(cause != null) initCause(cause)
  def this(errorCode: Int, desc: String) = this(errorCode, desc, null)
}