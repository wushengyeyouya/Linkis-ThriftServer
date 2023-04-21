package org.apache.linkis.thriftserver.service.client

import java.util

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
trait TableResultSetIterator extends Iterator[util.List[Any]] with java.util.Iterator[util.List[Any]] {

  def getMetadata: util.List[util.Map[String, String]]

}
