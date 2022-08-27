package org.apache.linkis.thriftserver.service.session

import java.io.File
import java.util
import java.util.concurrent.ThreadPoolExecutor

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.cli.session.{HiveSession, SessionManager}
import org.apache.hive.service.cli.{HiveSQLException, SessionHandle}
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.server.HiveServer2
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration
import org.apache.linkis.thriftserver.service.operation.LinkisOperationManager
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

/**
 *
 * @date 2022-08-10
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisSessionManager(linkisThriftServer2: HiveServer2)
  extends SessionManager(linkisThriftServer2) with Logging {

  private var operationManager: LinkisOperationManager = _
  private var isOperationLogEnabled: Boolean = false
  private var operationLogRootDir: File = _
  private var handleToSession: util.Map[SessionHandle, HiveSession] = _

  override def init(hiveConf: HiveConf): Unit = {
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE, false)
    hiveConf.setVar(ConfVars.HIVE_SERVER2_SESSION_HOOK, LinkisThriftServerConfiguration.SESSION_HOOK.getValue)
    operationManager = new LinkisOperationManager
    ReflectiveUtils.writeSuperField(this, "operationManager", operationManager)
    super.init(hiveConf)
    handleToSession = ReflectiveUtils.readSuperField(this, "handleToSession")
    initOperationLogRootDir()
    initOperationThreadPool()
  }

  private def initOperationLogRootDir(): Unit = {
    isOperationLogEnabled = LinkisThriftServerConfiguration.OPERATION_LOG_ENABLED.getValue
    if(!isOperationLogEnabled) {
      warn(s"Operation log is not enabled.")
      return
    }
    operationLogRootDir = new File(LinkisThriftServerConfiguration.OPERATION_LOG_LOCATION.getValue)
    if (operationLogRootDir.exists && !operationLogRootDir.isDirectory) {
      warn(s"Operation log root directory is not a directory with ${operationLogRootDir.getAbsolutePath}.")
      isOperationLogEnabled = false
    } else if (!operationLogRootDir.exists && !operationLogRootDir.mkdirs) {
      warn(s"Create operation log root directory failed with ${operationLogRootDir.getAbsolutePath}.")
      isOperationLogEnabled = false
    }
    if (isOperationLogEnabled) {
      info("Operation log root directory is created with " + operationLogRootDir.getAbsolutePath)
      Utils.tryAndWarnMsg(FileUtils.forceDeleteOnExit(operationLogRootDir))(s"Failed to delete operation log root dir with ${operationLogRootDir.getAbsolutePath}.")
    }
    ReflectiveUtils.writeSuperField(this, "isOperationLogEnabled", isOperationLogEnabled)
    ReflectiveUtils.writeSuperField(this, "operationLogRootDir", operationLogRootDir)
  }

  protected def initOperationThreadPool(): Unit = {
    val operationThreadPool = Utils.newCachedThreadPool(LinkisThriftServerConfiguration.ASYNC_EXEC_THREAD_SIZE.getValue, "Linkis-ThriftServer2-Background-Pool", true)
    operationThreadPool.allowCoreThreadTimeOut(true)
    val checkInterval = LinkisThriftServerConfiguration.SESSION_CHECK_INTERVAL.getValue.toLong
    val sessionTimeout = LinkisThriftServerConfiguration.IDLE_SESSION_TIMEOUT.getValue.toLong
    val checkEnable = LinkisThriftServerConfiguration.IDLE_SESSION_CHECK_ENABLE.getValue
    info("Shutdown the deprecated backgroundOperationPool of HiveSessionManager...")
    ReflectiveUtils.readSuperField[ThreadPoolExecutor](this, "backgroundOperationPool").shutdown()
    info(s"Try to create backgroundOperationPool with thread size ${LinkisThriftServerConfiguration.ASYNC_EXEC_THREAD_SIZE.getValue}.")
    ReflectiveUtils.writeSuperField(this, "backgroundOperationPool", operationThreadPool)
    ReflectiveUtils.writeSuperField(this, "checkInterval", checkInterval)
    ReflectiveUtils.writeSuperField(this, "sessionTimeout", sessionTimeout)
    ReflectiveUtils.writeSuperField(this, "checkOperation", checkEnable)
  }

  override def createSession(sessionHandle: SessionHandle, protocol: TProtocolVersion,
                             username: String, password: String, ipAddress: String,
                             sessionConf: util.Map[String, String], withImpersonation: Boolean,
                             delegationToken: String): HiveSession = {
    info(s"Try to open a new Session for user $username, ip: $ipAddress, sessionConf: $sessionConf, delegationToken: $delegationToken.")
    // 创建一个 Session
    val session = new LinkisSessionImpl(protocol, username, getHiveConf, ipAddress)
    session.setSessionManager(this)
    session.setOperationManager(operationManager)
    if (this.isOperationLogEnabled) {
      session.setOperationLogSessionDir(this.operationLogRootDir)
    }
    Utils.tryThrow(session.open(sessionConf)){e =>
      Utils.tryQuietly(session.close())
      new HiveSQLException("Failed to open new session: " + ExceptionUtils.getRootCauseMessage(e), e)
    }
    Utils.tryThrow(executeSessionHooks(session)){ e =>
      warn(s"Failed to execute session hooks for user $username.", e)
      Utils.tryQuietly(session.close())
      new HiveSQLException("Failed to execute session hooks: " + ExceptionUtils.getRootCauseMessage(e), e)
    }
    handleToSession.put(session.getSessionHandle, session)
    info(s"Opened a new Session ${session.getSessionHandle} for user: $username, ip: $ipAddress, current sessions num: $getOpenSessionCount.")
    session
  }

  protected def executeSessionHooks(hiveSession: HiveSession): Unit = {
//    ReflectiveUtils.invokeSetter(this, "executeSessionHooks", hiveSession)
    val executeSessionHooksMethod = classOf[SessionManager].getDeclaredMethod("executeSessionHooks", classOf[HiveSession])
    executeSessionHooksMethod.setAccessible(true)
    executeSessionHooksMethod.invoke(this, hiveSession)
  }
}
