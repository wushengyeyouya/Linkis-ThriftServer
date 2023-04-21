package org.apache.linkis.thriftserver.entrance.job

import java.util

import org.apache.commons.lang3.ArrayUtils
import org.apache.linkis.common.log.LogUtils
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.engineconn.cache.protocol.{FsPathListAndFirstPageResultSetListResponse, ResultSetResponseProtocol}
import org.apache.linkis.entrance.EntranceServer
import org.apache.linkis.entrance.execute.EntranceJob
import org.apache.linkis.protocol.constants.TaskConstant
import org.apache.linkis.rpc.Sender
import org.apache.linkis.scheduler.listener.JobListener
import org.apache.linkis.scheduler.queue.Job
import org.apache.linkis.thriftserver.conf.ThriftServerConfiguration
import org.apache.linkis.thriftserver.exception.EntranceThriftServerErrorException
import org.apache.linkis.thriftserver.orchestrator.operation.{ResultSetCacheEndpoint, ResultSetCacheOperation}
import org.apache.linkis.thriftserver.service.client._

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
class EntranceThriftServerLinkisJob(entranceServer: EntranceServer,
                                    executeUser: String,
                                    submitUser: String,
                                    creator: String,
                                    labelMap: util.HashMap[String, Any],
                                    jobContent: util.HashMap[String, Any],
                                    sourceMap: util.HashMap[String, Any],
                                    startupMap: util.HashMap[String, Any],
                                    variableMap: util.HashMap[String, Any],
                                    runtimeMap: util.HashMap[String, Any]) extends ThriftServerLinkisJob with Logging {

  private var id: String = _
  private var job: EntranceJob = _
  private val startTime = System.currentTimeMillis
  private val lock = new Object
  private var resultSetCacheEndpoint: ResultSetCacheEndpoint = _
  private var response: FsPathListAndFirstPageResultSetListResponse = _

  override def getId: String = id

  override def submit(): Unit = {
    val requestBody = new util.HashMap[String, Any]
    requestBody.put(TaskConstant.EXECUTION_CONTENT, jobContent)
    requestBody.put(TaskConstant.EXECUTE_USER, executeUser)
    requestBody.put(TaskConstant.SUBMIT_USER, submitUser)
    requestBody.put(TaskConstant.SOURCE, sourceMap)
    requestBody.put(TaskConstant.LABELS, labelMap)
    val params = new util.HashMap[String, Any]
    requestBody.put(TaskConstant.PARAMS, params)
    id = entranceServer.execute(requestBody)
    entranceServer.getJob(id).foreach {
      case job: EntranceJob => this.job = job
    }
    val jobListener = job.getJobListener
    job.setJobListener(new JobListener {
      override def onJobScheduled(job: Job): Unit = jobListener.foreach(_.onJobScheduled(job))
      override def onJobInited(job: Job): Unit = jobListener.foreach(_.onJobInited(job))
      override def onJobWaitForRetry(job: Job): Unit = jobListener.foreach(_.onJobWaitForRetry(job))
      override def onJobRunning(job: Job): Unit = jobListener.foreach(_.onJobRunning(job))
      override def onJobCompleted(job: Job): Unit = {
        Utils.tryFinally(jobListener.foreach(_.onJobCompleted(job))){
          lock synchronized lock.notifyAll()
        }
      }
    })
    info(s"User(submitUser: $submitUser, executeUser: $executeUser) submitted query with job messages => jobId: $id, jobReqId: ${job.jobRequest.getId}, from source $sourceMap.")
    generateLog("You have submitted a new job, script code (after variable substitution) is")
    generateLog("************************************SCRIPT CODE************************************")
    generateLog(job.jobRequest.getExecutionCode)
    generateLog("************************************SCRIPT CODE************************************")
    generateLog(s"Your job is accepted, please wait it to be scheduled.")
    generateLog(LogUtils.generateSystemInfo(s"Useful job messages => jobId: $id, jobReqId: ${job.jobRequest.getId}, Entrance: ${Sender.getThisServiceInstance.toString}."))
  }

  private def generateLog(log: String): Unit = {
    entranceServer.getEntranceContext.getOrCreateLogManager().onLogUpdate(job, log)
  }

  override def waitForCompleted(): Unit = {
    def isTimeout: Boolean = System.currentTimeMillis() - startTime > ThriftServerConfiguration.JOB_MAX_EXECUTION_TIME.getValue.toLong
    lock synchronized {
      while(!job.isCompleted && !isTimeout) {
        lock wait 2000
        job.updateNewestAccessByClientTimestamp()
      }
    }
    if(job.isSucceed) {
      job.operation(_.getEngineExecuteAsyncReturn.foreach(_.getOrchestrationFuture().foreach{ future =>
        resultSetCacheEndpoint = future.operate(ResultSetCacheOperation.OPERATION_NAME)
        info(s"Job $id was completed with $resultSetCacheEndpoint.")
      }))
    }
  }

  override def kill(): Unit = job.kill()

  override def getJobInfo: LinkisJobInfo = new LinkisJobInfo {
    override def getErrCode: Int = job.getJobRequest.getErrorCode

    override def getErrDesc: String = job.getJobRequest.getErrorDesc

    override def getProgress: Double = job.getProgress

    override def getStatus: String = job.getState.toString
  }

  override def isCompleted: Boolean = job.isCompleted

  override def isSucceed: Boolean = job.isSucceed

  override def existResultSets: Boolean = {
    if(response == null) {
      response = resultSetCacheEndpoint.getFsPathListAndFirstPage(job.jobRequest.getResultLocation)
    }
    ArrayUtils.isNotEmpty(response.resultSetPaths)
  }

  override def getResultSetIterables: Array[ResultSetIterable] = if(isSucceed) {
    if(existResultSets) response.resultSetPaths.indices.map { inx =>
      val fsPath = response.resultSetPaths(inx)
      new ResultSetIterable {
        override def getFsPath: String = fsPath
        override def iterator: TableResultSetIterator = new TableResultSetIterator {
          private var (resultSetResponse, metadata): (ResultSetResponseProtocol, util.List[util.Map[String, String]]) =
            if(inx == 0) (response, util.Arrays.asList(response.metadata: _*))
            else {
              val firstPageResponse = resultSetCacheEndpoint.getFirstPage(fsPath)
              (firstPageResponse, util.Arrays.asList(firstPageResponse.metadata: _*))
            }
          private var index = 0

          override def getMetadata: util.List[util.Map[String, String]] = metadata

          override def hasNext: Boolean = if(index < resultSetResponse.records.length) true else if(resultSetResponse.hasNext) {
            resultSetResponse = resultSetCacheEndpoint.getNextPage(fsPath)
            index = 0
            ArrayUtils.isNotEmpty(resultSetResponse.records)
          } else false

          override def next(): util.List[Any] = {
            val record = resultSetResponse.records(index)
            index += 1
            util.Arrays.asList(record:_*)
          }
        }
      }
    }.toArray else Array.empty
  } else if(isCompleted) {
    throw new EntranceThriftServerErrorException(80005, s"Job $id is failed, cannot fetch result sets.")
  } else {
    throw new EntranceThriftServerErrorException(80005, s"Job $id is running, cannot fetch result sets.")
  }

  override def addLogListener(logListener: LogListener): Unit = {
    val originLogListener = job.getLogListener
    job.setLogListener(new org.apache.linkis.scheduler.listener.LogListener {
      override def onLogUpdate(job: Job, log: String): Unit = {
        originLogListener.foreach(_.onLogUpdate(job, log))
        logListener.onLogUpdate(util.Arrays.asList(log))
      }
    })
  }

  override def cleanup(): Unit = {}
}
