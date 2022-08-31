package org.apache.linkis.thriftserver.service.operation.handler
import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration
import org.apache.linkis.thriftserver.service.session.LinkisSessionImpl

/**
 *
 * @date 2022-08-30
 * @author enjoyyin
 * @since 0.5.0
 */
object ProxyUserUtils {

  private val PROXY_USER = "proxy.user"

  def getProxyUser(statement: Statement): String = statement.getProperties.get(PROXY_USER)

  def getExecuteUser(session: HiveSession, statement: Statement): String = if(existsProxyUser(statement)) {
    getProxyUser(statement)
  } else session.getUserName

  def existsProxyUser(statement: Statement): Boolean = statement.getProperties.containsKey(PROXY_USER)

  def getJobCreator(session: HiveSession, statement: Statement): String = {
    if(statement.getProperties.containsKey(LinkisThriftServerConfiguration.JOB_CREATOR.key))
      LinkisThriftServerConfiguration.JOB_CREATOR.getValue(statement.getProperties)
    else session match {
      case linkisSessionImpl: LinkisSessionImpl => linkisSessionImpl.getCreator
    }
  }
  
}