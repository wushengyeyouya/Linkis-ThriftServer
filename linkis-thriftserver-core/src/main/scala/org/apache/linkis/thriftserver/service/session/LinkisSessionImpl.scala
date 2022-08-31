package org.apache.linkis.thriftserver.service.session

import java.io.File
import java.util
import java.util.concurrent.Semaphore

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.IMetaStoreClient
import org.apache.hive.service.auth.HiveAuthFactory
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.OperationManager
import org.apache.hive.service.cli.session.HiveSessionImpl
import org.apache.hive.service.rpc.thrift.TProtocolVersion
import org.apache.hive.service.server.ThreadWithGarbageCleanup
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration
import org.apache.linkis.thriftserver.exception.LinkisThriftServerNotSupportException
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-11
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisSessionImpl(protocol: TProtocolVersion, username: String, hiveConf: HiveConf, ipAddress: String)
  extends HiveSessionImpl(null, protocol, username, "", hiveConf, ipAddress) with Logging {

  private var sessionConfMap: util.Map[String, String] = _
  private var operationLock: Semaphore = _
  private var opHandleSet: util.Set[OperationHandle] = _
  private var operationManager: OperationManager = _
  private val currentDBs = new util.HashMap[String, String]()

  override def open(map: util.Map[String, String]): Unit = {
    this.sessionConfMap = map
    setLastAccessTime()
    setLastIdleTime()
    operationLock = ReflectiveUtils.readSuperField(this, "operationLock")
    opHandleSet = ReflectiveUtils.readSuperField(this, "opHandleSet")
  }

  def getEngineType: String = LinkisThriftServerConfiguration.ENGINE_TYPE.getValue(sessionConfMap)

  def getEngineVersion: String = LinkisThriftServerConfiguration.ENGINE_VERSION.getValue(sessionConfMap)

  def getRunTypeStr: String = LinkisThriftServerConfiguration.runTypeMap(getEngineType)

  def getCreator: String = LinkisThriftServerConfiguration.JOB_CREATOR.getValue(sessionConfMap)

  private def setLastAccessTime(): Unit = {
    ReflectiveUtils.writeSuperField(this, "lastAccessTime", System.currentTimeMillis)
  }

  private def resetLastIdleTime(): Unit = {
    ReflectiveUtils.writeSuperField(this, "lastIdleTime", 0)
  }

  private def setLastIdleTime(): Unit = {
    ReflectiveUtils.writeSuperField(this, "lastIdleTime", System.currentTimeMillis)
  }

  private def getActiveCalls: Int = {
    ReflectiveUtils.readSuperField(this, "activeCalls")
  }

  private def setActiveCalls(value: Int): Unit = {
    ReflectiveUtils.writeSuperField(this, "activeCalls", value)
  }

  override protected def acquire(userAccess: Boolean, isOperation: Boolean): Unit = {
    if (isOperation && this.operationLock != null) Utils.tryThrow(this.operationLock.acquire()) { e =>
        Thread.currentThread.interrupt()
        new RuntimeException(e)
    }
    if (userAccess) setLastAccessTime()
    resetLastIdleTime()
    setActiveCalls(getActiveCalls + 1)
  }

  override protected def release(userAccess: Boolean, isOperation: Boolean): Unit = Utils.tryFinally {
    Thread.currentThread() match {
      case currentThread: ThreadWithGarbageCleanup =>
        currentThread.cacheThreadLocalRawStore()
      case _ =>
    }
    setActiveCalls(getActiveCalls - 1)
    if (getActiveCalls == 0 && this.opHandleSet.isEmpty) setLastIdleTime()
  } {
    if (isOperation && this.operationLock != null) this.operationLock.release()
  }

  override def getMetaStoreClient: IMetaStoreClient = throw new LinkisThriftServerNotSupportException("getMetaStoreClient")

  private def acquireAndRelease[T](op: => T, userAccess: Boolean, isOperation: Boolean): T = Utils.tryFinally{
    this.acquire(userAccess, isOperation)
    op
  }(this.release(userAccess, isOperation))

  override def getColumns(catalogName: String, schemaName: String, tableName: String, columnName: String): OperationHandle = acquireAndRelease({
    val operation = operationManager.newGetColumnsOperation(this.getSession, catalogName, schemaName, tableName, columnName)
    val opHandle = operation.getHandle
    Utils.tryThrow {
      operation.run()
      this.opHandleSet synchronized this.opHandleSet.add(opHandle)
    } {e =>
      operationManager.closeOperation(opHandle)
      e
    }
    opHandle
  }, true, true)

  override def close(): Unit = acquireAndRelease({
    val ops = this.opHandleSet synchronized new util.ArrayList[OperationHandle](this.opHandleSet)
    this.opHandleSet.clear()
    ops.asScala.foreach { opHandle =>
      operationManager.closeOperation(opHandle)
    }
    if (this.isOperationLogEnabled) Utils.tryCatch {
      val sessionLogDir: File = ReflectiveUtils.readSuperField(this, "sessionLogDir")
      FileUtils.forceDelete(sessionLogDir)
      info("Operation log session directory is deleted: " + sessionLogDir.getAbsolutePath)
    } { e =>
        error("Failed to cleanup session log dir: " + this.getSessionHandle, e)
    }
  }, true, false)

  override def setOperationManager(operationManager: OperationManager): Unit = {
    super.setOperationManager(operationManager)
    this.operationManager = operationManager
  }

  override def getDelegationToken(authFactory: HiveAuthFactory, owner: String, renewer: String): String =
    throw new LinkisThriftServerNotSupportException("getDelegationToken")

  override def cancelDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit =
    throw new LinkisThriftServerNotSupportException("cancelDelegationToken")

  override def renewDelegationToken(authFactory: HiveAuthFactory, tokenStr: String): Unit =
    throw new LinkisThriftServerNotSupportException("renewDelegationToken")

  def setCurrentDB(currentUser: String, currentDB: String): Unit = synchronized {
    info(s"User $currentUser in Session $getSessionHandle switch db to $currentDB.")
    this.currentDBs.put(currentUser, currentDB)
  }

  def getCurrentDB(currentUser: String): String = currentDBs.get(currentUser)

}