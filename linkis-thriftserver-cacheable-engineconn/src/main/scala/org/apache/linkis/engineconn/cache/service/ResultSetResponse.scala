package org.apache.linkis.engineconn.cache.service

import org.apache.linkis.engineconn.cache.protocol.ResultSetResponseProtocol

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
trait ResultSetResponse {
  val response: ResultSetResponseProtocol
  val readBytes: Int
  val readTime: Long
}

case class ResultSetResponseImpl(response: ResultSetResponseProtocol,
                                 readBytes: Int,
                                 readTime: Long) extends ResultSetResponse