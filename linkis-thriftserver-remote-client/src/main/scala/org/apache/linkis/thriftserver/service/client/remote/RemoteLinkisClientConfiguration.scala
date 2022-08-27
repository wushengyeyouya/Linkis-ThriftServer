package org.apache.linkis.thriftserver.service.client.remote

import org.apache.linkis.common.conf.CommonVars

/**
 *
 * @date 2022-08-19
 * @author enjoyyin
 * @since 0.5.0
 */
object RemoteLinkisClientConfiguration {

  val LINKIS_GATEWAY_URL = CommonVars("linkis.gateway.url", "")

}
