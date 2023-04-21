package org.apache.linkis.thriftserver.entrance.job

import java.util

import org.apache.linkis.entrance.EntranceServer
import org.apache.linkis.thriftserver.service.client.ThriftServerLinkisJobBuilder

/**
 *
 * @date 2022-09-30
 * @author enjoyyin
 * @since 0.5.0
 */
class EntranceThriftServerLinkisJobBuilder(entranceServer: EntranceServer) extends ThriftServerLinkisJobBuilder {

  protected var executeUser: String = _
  protected var submitUser: String = _
  protected var creator: String = _
  protected val labelMap = new util.HashMap[String, Any]
  protected val jobContent = new util.HashMap[String, Any]
  protected val sourceMap = new util.HashMap[String, Any]
  protected val startupMap = new util.HashMap[String, Any]
  protected val variableMap = new util.HashMap[String, Any]
  protected val runtimeMap = new util.HashMap[String, Any]

  override def addExecuteUser(executeUser: String): EntranceThriftServerLinkisJobBuilder = {
    this.executeUser = executeUser
    this
  }

  override def addSubmitUser(submitUser: String): EntranceThriftServerLinkisJobBuilder = {
    this.submitUser = submitUser
    this
  }

  override def addLabel(key: String, value: Any): EntranceThriftServerLinkisJobBuilder = {
    labelMap.put(key, value)
    this
  }

  override def addEngineType(engineType: String): EntranceThriftServerLinkisJobBuilder = this

  override def addCreator(creator: String): EntranceThriftServerLinkisJobBuilder = {
    this.creator = creator
    this
  }

  override def addJobContent(key: String, value: Any): EntranceThriftServerLinkisJobBuilder = {
    jobContent.put(key, value)
    this
  }

  override def addSource(key: String, value: Any): EntranceThriftServerLinkisJobBuilder = {
    sourceMap.put(key, value)
    this
  }

  override def addVariable(key: String, value: Any): EntranceThriftServerLinkisJobBuilder = {
    variableMap.put(key, value)
    this
  }

  override def addStartupParam(key: String, value: Any): EntranceThriftServerLinkisJobBuilder = {
    startupMap.put(key, value)
    this
  }

  override def addRuntimeParam(key: String, value: Any): EntranceThriftServerLinkisJobBuilder = {
    runtimeMap.put(key, value)
    this
  }

  override def build(): EntranceThriftServerLinkisJob = new EntranceThriftServerLinkisJob(entranceServer, executeUser, submitUser,
    creator, labelMap, jobContent, sourceMap, startupMap, variableMap, runtimeMap)

}
