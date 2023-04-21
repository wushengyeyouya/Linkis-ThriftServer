package org.apache.linkis.thriftserver.service.client

/**
 *
 * @date 2022-09-30
 * @author enjoyyin
 * @since 0.5.0
 */
trait ThriftServerLinkisJob {

  def getId: String

  def submit(): Unit

  def waitForCompleted(): Unit

  def kill(): Unit

  def getJobInfo: LinkisJobInfo

  def isCompleted: Boolean

  def isSucceed: Boolean

  def existResultSets: Boolean

  def getResultSetIterables: Array[ResultSetIterable]

  def addLogListener(logListener: LogListener): Unit

  def cleanup(): Unit

}

trait LinkisJobInfo {

  def getErrCode: Int

  def getErrDesc: String

  def getProgress: Double

  def getStatus: String

}
