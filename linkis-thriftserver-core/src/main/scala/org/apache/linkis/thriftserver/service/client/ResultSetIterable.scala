package org.apache.linkis.thriftserver.service.client

import java.util

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
trait ResultSetIterable {

  def getFsPath: String

  def iterator: TableResultSetIterator

}
