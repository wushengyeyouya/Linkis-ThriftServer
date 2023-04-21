package org.apache.linkis.thriftserver.service.client

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
trait ThriftServerLinkisJobBuilder {

  def addExecuteUser(executeUser: String): this.type

  def addSubmitUser(submitUser: String): this.type

  def addLabel(key: String, value: Any): this.type

  def addEngineType(engineType: String): this.type

  def addCreator(creator: String): this.type

  def addJobContent(key: String, value: Any): this.type

  def addSource(key: String, value: Any): this.type

  def addVariable(key: String, value: Any): this.type

  def addStartupParam(key: String, value: Any): this.type

  def addRuntimeParam(key: String, value: Any): this.type

  def build(): ThriftServerLinkisJob

}