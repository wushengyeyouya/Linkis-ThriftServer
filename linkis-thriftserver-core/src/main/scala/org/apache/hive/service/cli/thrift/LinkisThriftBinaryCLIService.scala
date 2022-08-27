package org.apache.hive.service.cli.thrift

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.util.StringUtils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.rpc.thrift._
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration
import org.apache.linkis.thriftserver.service.operation.LinkisSQLExecuteStatementOperation

/**
 *
 * @date 2022-08-25
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisThriftBinaryCLIService(cliService: CLIService, oomHook: Runnable)
  extends ThriftBinaryCLIService(cliService, oomHook) with Logging {

  override def init(hiveConf: HiveConf): Unit = {
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, getAuthentication)
    info(s"set ${ConfVars.HIVE_SERVER2_AUTHENTICATION.varname}=$getAuthentication.")
    super.init(hiveConf)
  }

  protected def getAuthentication: String = LinkisThriftServerConfiguration.AUTHENTICATION.getValue

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    val resp = new TGetOperationStatusResp
    val operationHandle = new OperationHandle(req.getOperationHandle)
    Utils.tryCatch {
      val operationStatus = this.cliService.getOperationStatus(operationHandle, req.isGetProgressUpdate)
      resp.setOperationState(operationStatus.getState.toTOperationState)
      val opException = operationStatus.getOperationException
      resp.setTaskStatus(operationStatus.getTaskStatus)
      resp.setOperationStarted(operationStatus.getOperationStarted)
      resp.setOperationCompleted(operationStatus.getOperationCompleted)
      resp.setHasResultSet(operationStatus.getHasResultSet)
      val progressUpdate = operationStatus.jobProgressUpdate
      val executionStatus = ProgressMonitorStatusMapper.DEFAULT.forStatus(progressUpdate.status)
      resp.setProgressUpdateResponse(new TProgressUpdateResp(progressUpdate.headers, progressUpdate.rows,
        progressUpdate.progressedPercentage, executionStatus, progressUpdate.footerSummary, progressUpdate.startTimeMillis))
      if (opException != null) {
        resp.setSqlState(opException.getSQLState)
        resp.setErrorCode(opException.getErrorCode)
        resp.setErrorMessage(StringUtils.stringifyException(opException))
      } else if (executionStatus == TJobExecutionStatus.NOT_AVAILABLE && OperationType.EXECUTE_STATEMENT == operationHandle.getOperationType) {
        val progress = cliService.getSessionManager.getOperationManager.getOperation(operationHandle) match {
          case operation: LinkisSQLExecuteStatementOperation => operation.getProgress
          case _: ExecuteStatementOperation => 0.5d
        }
        resp.getProgressUpdateResponse.setProgressedPercentage(progress)
      }
    } { case e: Exception =>
      warn("Error when getting operation status: ", e)
      resp.setStatus(HiveSQLException.toTStatus(e))
      return resp
    }
    resp.setStatus(new TStatus(TStatusCode.SUCCESS_STATUS))
    resp
  }

}

object LinkisThriftBinaryCLIService extends Logging {

  def apply(cliService: CLIService, oomHook: Runnable): LinkisThriftBinaryCLIService = {
    val classStr = LinkisThriftServerConfiguration.LINKIS_BINARY_CLI_SERVICE_CLASS.getValue
    val create: Class[_ <: LinkisThriftBinaryCLIService] => LinkisThriftBinaryCLIService = clazz => {
      info(s"Try to use $clazz to create the instance of LinkisThriftBinaryCLIService.")
      clazz.getConstructor(classOf[CLIService], classOf[Runnable]).newInstance(cliService, oomHook)
    }
    if(org.apache.commons.lang3.StringUtils.isNotBlank(classStr)) {
      create(Class.forName(classStr).asInstanceOf[Class[LinkisThriftBinaryCLIService]])
    } else {
      info(s"Use LinkisThriftBinaryCLIService to create the instance.")
      new LinkisThriftBinaryCLIService(cliService, oomHook)
    }
  }

}