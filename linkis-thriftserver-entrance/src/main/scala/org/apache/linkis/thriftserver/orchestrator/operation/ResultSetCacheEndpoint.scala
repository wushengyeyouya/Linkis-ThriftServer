package org.apache.linkis.thriftserver.orchestrator.operation

import org.apache.linkis.engineconn.cache.protocol._
import org.apache.linkis.orchestrator.computation.physical.CodeLogicalUnitExecTask
import org.apache.linkis.orchestrator.ecm.service.EngineConnExecutor
import org.apache.linkis.orchestrator.ecm.service.impl.ComputationEngineConnExecutor
import org.apache.linkis.orchestrator.plans.physical.ExecTask
import org.apache.linkis.orchestrator.{Orchestration, OrchestrationUtils}
import org.apache.linkis.rpc.Sender
import org.apache.linkis.rpc.utils.RPCUtils
import org.apache.linkis.thriftserver.conf.ThriftServerConfiguration.THRIFT_SERVER_RESULT_SET_PAGE_SIZE
import org.apache.linkis.thriftserver.exception.EntranceThriftServerErrorException

/**
 *
 * @date 2022-12-23
 * @author enjoyyin
 * @since 0.5.0
 */
trait ResultSetCacheEndpoint {

  def getFsPathListAndFirstPage(parentFsPath: String): FsPathListAndFirstPageResultSetListResponse

  def getFirstPage(fsPath: String): FirstPageResultSetListResponse

  def getNextPage(fsPath: String): ResultSetListResponse

}

class ResultSetCacheEndpointImpl(orchestration: Orchestration) extends ResultSetCacheEndpoint {

  private val sender = findCodeLogicalUnitExecTask(OrchestrationUtils.getPhysicalPlan(orchestration)).map { execTask =>
    val executor = execTask.getCodeEngineConnExecutor
    ResultSetCacheEndpointImpl.getEngineConnSender(executor.getEngineConnExecutor)
  }.getOrElse(throw new EntranceThriftServerErrorException(60020, "cannot find available engineConn Sender, please ask admin for help!"))

  private def findCodeLogicalUnitExecTask(task: ExecTask): Option[CodeLogicalUnitExecTask] = {
    task.getChildren.foreach {
      case t: CodeLogicalUnitExecTask => return Some(t)
      case t => return findCodeLogicalUnitExecTask(t)
    }
    None
  }

  private def ask[T](requestEntity: Int => Any): T = sender.ask(requestEntity(THRIFT_SERVER_RESULT_SET_PAGE_SIZE.getValue)) match {
    case response: T => response
    case e: Exception => throw e
  }

  override def getFsPathListAndFirstPage(parentFsPath: String): FsPathListAndFirstPageResultSetListResponse =
    ask(FsPathListAndFirstPageRequest(parentFsPath, _))

  override def getFirstPage(fsPath: String): FirstPageResultSetListResponse = ask(FirstPageRequest(fsPath, _))

  override def getNextPage(fsPath: String): ResultSetListResponse = ask(NextPageRequest(fsPath, _))

  override val toString: String = s"ResultSetCacheEndpoint(${RPCUtils.getServiceInstanceFromSender(sender)})"
}

object ResultSetCacheEndpointImpl {

  private val getEngineConnSenderMethod = classOf[ComputationEngineConnExecutor].getMethod("getEngineConnSender")
  getEngineConnSenderMethod.setAccessible(true)

  def getEngineConnSender(engineConnExecutor: EngineConnExecutor): Sender = getEngineConnSenderMethod.invoke(engineConnExecutor) match {
    case sender: Sender => sender
  }

}