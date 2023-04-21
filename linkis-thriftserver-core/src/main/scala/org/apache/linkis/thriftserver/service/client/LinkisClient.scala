package org.apache.linkis.thriftserver.service.client

import org.apache.commons.lang3.StringUtils
import org.apache.linkis.common.utils.{ClassUtils, Logging}
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-17
 * @author enjoyyin
 * @since 0.5.0
 */
trait LinkisClient {

  def init(): Unit

  def newLinkisJobBuilder(): ThriftServerLinkisJobBuilder

  def getMetaDataClient(user: String): MetadataClient

}

object LinkisClient extends Logging {

  private val linkisClient: LinkisClient = {
    val clientClass = LinkisThriftServerConfiguration.LINKIS_JOB_CLIENT_CLASS.getValue
    val client = if(StringUtils.isNotEmpty(clientClass)) {
      info(s"Try to use $clientClass to create the instance of LinkisClient.")
      ClassUtils.getClassInstance(clientClass)
    } else {
      val clientClasses = ClassUtils.reflections.getSubTypesOf(classOf[LinkisClient]).asScala.filterNot(ClassUtils.isInterfaceOrAbstract).toSeq
      if(clientClasses.isEmpty) {
        throw new Error("Not exists LinkisClient.")
      } else if(clientClasses.size > 1) {
        throw new Error("too much subclasses for LinkisClient, found: " + clientClasses)
      } else {
        info(s"Try to use ${clientClasses.head} to create the instance of LinkisClient.")
        clientClasses.head.newInstance()
      }
    }
    client.init()
    client
  }

  def getLinkisClient: LinkisClient = linkisClient

}