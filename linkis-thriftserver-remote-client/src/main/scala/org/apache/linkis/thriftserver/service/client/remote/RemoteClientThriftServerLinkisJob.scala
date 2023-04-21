package org.apache.linkis.thriftserver.service.client.remote

import java.util

import org.apache.linkis.computation.client
import org.apache.linkis.computation.client.interactive
import org.apache.linkis.computation.client.interactive.SubmittableInteractiveJob
import org.apache.linkis.thriftserver.exception.LinkisThriftServerWarnException
import org.apache.linkis.thriftserver.service.client._

/**
 *
 * @date 2022-09-30
 * @author enjoyyin
 * @since 0.5.0
 */
class RemoteClientThriftServerLinkisJob(linkisJob: SubmittableInteractiveJob) extends ThriftServerLinkisJob {

  override def getId: String = linkisJob.getId

  override def submit(): Unit = linkisJob.submit()

  override def waitForCompleted(): Unit = linkisJob.waitForCompleted()

  override def kill(): Unit = linkisJob.kill()

  override def getJobInfo: LinkisJobInfo = new LinkisJobInfo {
    override def getErrCode: Int = linkisJob.getJobInfo.getErrCode

    override def getErrDesc: String = linkisJob.getJobInfo.getErrDesc

    override def getProgress: Double = linkisJob.getJobInfo.getProgress.toDouble

    override def getStatus: String = linkisJob.getJobInfo.getStatus
  }

  override def isCompleted: Boolean = linkisJob.isCompleted

  override def isSucceed: Boolean = linkisJob.isSucceed

  override def existResultSets: Boolean = linkisJob.existResultSets

  override def getResultSetIterables: Array[ResultSetIterable] = linkisJob.getResultSetIterables.map {
    iterable => new RemoteClientResultSetIterable(iterable)
  }

  override def addLogListener(logListener: LogListener): Unit = linkisJob.addLogListener(new interactive.LogListener {
    override def onLogUpdate(logs: util.List[String]): Unit = logListener.onLogUpdate(logs)
  })

  override def cleanup(): Unit = {}
}

class RemoteClientResultSetIterable(iterable: client.ResultSetIterable) extends ResultSetIterable {

  private val taskIterator = iterable.iterator match {
    case tableResultSetIterator: client.TableResultSetIterator => tableResultSetIterator
    case t => throw new LinkisThriftServerWarnException(80001,
      "Not support resultSet type " + t.getClass.getSimpleName.replace("ResultSetIterator", ""))
  }

  override def getFsPath: String = iterable.getFsPath

  override def iterator: TableResultSetIterator = new TableResultSetIterator {
    override def getMetadata: util.List[util.Map[String, String]] = taskIterator.getMetadata

    override def hasNext: Boolean = taskIterator.hasNext()

    override def next(): util.List[Any] = taskIterator.next()
  }
}