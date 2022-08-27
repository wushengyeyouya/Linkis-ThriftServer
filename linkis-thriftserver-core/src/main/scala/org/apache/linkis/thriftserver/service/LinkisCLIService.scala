package org.apache.linkis.thriftserver.service

import java.util

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hive.service.cli.{CLIService, OperationHandle, SessionHandle}
import org.apache.hive.service.server.HiveServer2
import org.apache.linkis.common.utils.{ByteTimeUtils, Logging}
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration
import org.apache.linkis.thriftserver.service.session.LinkisSessionManager
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

/**
 *
 * @date 2022-08-10
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisCLIService(linkisThriftServer2: HiveServer2)
  extends CLIService(linkisThriftServer2) with LinkisCompositeService with Logging {

  private var sessionManager: LinkisSessionManager = _

  override def init(hiveConf: HiveConf): Unit = {
    sessionManager = createSessionManager(hiveConf)
    addService(sessionManager)
    ReflectiveUtils.writeSuperField(this, "sessionManager", sessionManager)
    val defaultFetchRows = LinkisThriftServerConfiguration.RESULTSET_DEFAULT_FETCH_SIZE.getValue
    ReflectiveUtils.writeSuperField(this, "defaultFetchRows", defaultFetchRows)
    initService(hiveConf)
  }

  protected def createSessionManager(hiveConf: HiveConf): LinkisSessionManager = new LinkisSessionManager(linkisThriftServer2)

  override def executeStatement(sessionHandle: SessionHandle, statement: String, confOverlay: util.Map[String, String]): OperationHandle = {
    val session = this.sessionManager.getSession(sessionHandle)
    info(s"User ${session.getUserName} with SessionId($sessionHandle) try to executeStatement: $statement.")
    val opHandle = session.executeStatement(statement, confOverlay)
    opHandle
  }

  override def executeStatement(sessionHandle: SessionHandle, statement: String, confOverlay: util.Map[String, String], queryTimeout: Long): OperationHandle = {
    val session = this.sessionManager.getSession(sessionHandle)
    info(s"User ${session.getUserName} with SessionId($sessionHandle) try to executeStatement: $statement.")
    val opHandle = session.executeStatement(statement, confOverlay, queryTimeout)
    opHandle
  }

  override def executeStatementAsync(sessionHandle: SessionHandle, statement: String, confOverlay: util.Map[String, String]): OperationHandle = {
    val session = this.sessionManager.getSession(sessionHandle)
    info(s"User ${session.getUserName} with SessionId($sessionHandle) try to executeStatementAsync: $statement.")
    val opHandle = session.executeStatementAsync(statement, confOverlay)
    opHandle
  }

  override def executeStatementAsync(sessionHandle: SessionHandle, statement: String, confOverlay: util.Map[String, String], queryTimeout: Long): OperationHandle = {
    val session = this.sessionManager.getSession(sessionHandle)
    info(s"User ${session.getUserName} with SessionId($sessionHandle) try to executeStatementAsync with queryTimeout ${ByteTimeUtils.msDurationToString(queryTimeout)}: $statement.")
    val opHandle = session.executeStatementAsync(statement, confOverlay, queryTimeout)
    opHandle
  }

}
