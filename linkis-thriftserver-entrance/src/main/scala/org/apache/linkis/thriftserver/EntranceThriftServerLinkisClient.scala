package org.apache.linkis.thriftserver

import org.apache.linkis.DataWorkCloudApplication
import org.apache.linkis.common.utils.{ClassUtils, Logging}
import org.apache.linkis.entrance.EntranceServer
import org.apache.linkis.thriftserver.entrance.job.EntranceThriftServerLinkisJobBuilder
import org.apache.linkis.thriftserver.service.client.{LinkisClient, MetadataClient, ThriftServerLinkisJobBuilder}

/**
 *
 * @date 2022-09-30
 * @author enjoyyin
 * @since 0.5.0
 */
class EntranceThriftServerLinkisClient extends LinkisClient with Logging {

  private var entranceServer: EntranceServer = _

  override def init(): Unit = if(entranceServer == null) {
    info("try to initialize Linkis ThriftServer...")
    entranceServer = DataWorkCloudApplication.getApplicationContext.getBean(classOf[EntranceServer])
    info("Linkis ThriftServer is inited.")
  }

  override def newLinkisJobBuilder(): ThriftServerLinkisJobBuilder =
    EntranceThriftServerLinkisClient.linkisJobBuilderConstructor.newInstance(entranceServer)

  override def getMetaDataClient(user: String): MetadataClient = new EntranceMetadataClient(user)

}

import scala.collection.JavaConverters._

object EntranceThriftServerLinkisClient extends Logging {

  private val linkisJobBuilderClass = ClassUtils.reflections.getSubTypesOf(classOf[ThriftServerLinkisJobBuilder])
    .asScala.filterNot(ClassUtils.isInterfaceOrAbstract).filterNot(_ == classOf[EntranceThriftServerLinkisJobBuilder])
    .headOption.getOrElse(classOf[EntranceThriftServerLinkisJobBuilder])
  private val linkisJobBuilderConstructor = linkisJobBuilderClass.getConstructor(classOf[EntranceServer])
  info(s"found $linkisJobBuilderClass, will try to use it to create ThriftServerLinkisJob instance.")

//  def setExecutor(executor: ComputationExecutor): Unit = {
//    this.executor = executor
//    synchronized {
//      notifyAll()
//    }
//  }
//
//  def getExecutor: ComputationExecutor = executor
//
//  def waitFor(): Unit = synchronized {
//    while(executor == null) {
//      wait(2000)
//    }
//  }
}