package org.apache.linkis.thriftserver.service.client.remote

import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.lang3.StringUtils
import org.apache.linkis.common.utils.Logging
import org.apache.linkis.computation.client.LinkisJobBuilder
import org.apache.linkis.thriftserver.exception.LinkisThriftServerWarnException
import org.apache.linkis.thriftserver.service.client.{LinkisClient, MetadataClient, ThriftServerLinkisJobBuilder}

/**
 *
 * @date 2022-08-19
 * @author enjoyyin
 * @since 0.5.0
 */
class RemoteLinkisClient extends LinkisClient with Logging {

  private val metadataClientMap = new ConcurrentHashMap[String, MetadataClient]

  override def init(): Unit = {
    val gatewayUrl = RemoteLinkisClientConfiguration.LINKIS_GATEWAY_URL.getValue
    if(StringUtils.isBlank(gatewayUrl)) throw new LinkisThriftServerWarnException(80000,
      RemoteLinkisClientConfiguration.LINKIS_GATEWAY_URL.key + " must be set.")
    LinkisJobBuilder.setDefaultServerUrl(gatewayUrl)
  }

  override def newLinkisJobBuilder(): ThriftServerLinkisJobBuilder = new RemoteClientThriftServerLinkisJobBuilder

  override def getMetaDataClient(user: String): MetadataClient = {
    if(metadataClientMap.containsKey(user)) return metadataClientMap.get(user)
    metadataClientMap synchronized {
      if(!metadataClientMap.containsKey(user)) metadataClientMap.put(user, new RemoteMetadataClient(user, LinkisJobBuilder.getDefaultUJESClient))
    }
    metadataClientMap.get(user)
  }
}
