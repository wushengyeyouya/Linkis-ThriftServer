package org.apache.linkis.engineconn.cache.exception

import org.apache.linkis.common.exception.ErrorException

/**
 *
 * @date 2022-12-22
 * @author enjoyyin
 * @since 0.5.0
 */
class ResultSetCacheErrorException(errorCode: Int, desc: String, cause: Throwable) extends ErrorException(errorCode, desc) {
  if(cause != null) initCause(cause)
  def this(errorCode: Int, desc: String) = this(errorCode, desc, null)
}