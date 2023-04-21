package org.apache.linkis.thriftserver.service.client

import java.util

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
trait LogListener {

  def onLogUpdate(logs: util.List[String]): Unit

}
