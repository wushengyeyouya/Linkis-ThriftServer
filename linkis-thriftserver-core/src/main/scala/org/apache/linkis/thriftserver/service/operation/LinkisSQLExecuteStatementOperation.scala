package org.apache.linkis.thriftserver.service.operation

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Schema}
import org.apache.hadoop.hive.ql.session.OperationLog
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.SQLOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.computation.client.TableResultSetIterator
import org.apache.linkis.computation.client.interactive.{LogListener, SubmittableInteractiveJob}
import org.apache.linkis.computation.client.utils.LabelKeyUtils
import org.apache.linkis.thriftserver.service.client.LinkisClient
import org.apache.linkis.thriftserver.service.session.LinkisSessionImpl

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-10
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisSQLExecuteStatementOperation(parentSession: HiveSession,
                                         statement: String,
                                         confOverlay: util.Map[String, String],
                                         runInBackground: Boolean,
                                         queryTimeout: Long)
  extends SQLOperation(parentSession, statement, confOverlay, runInBackground, queryTimeout) with Logging {

  private val linkisSessionImpl: LinkisSessionImpl = parentSession match {
    case sessionImpl: LinkisSessionImpl => sessionImpl
  }
  private var linkisJob: SubmittableInteractiveJob = _
  private var resultIterator: TableResultSetIterator = _

  override protected def runQuery(): Unit = {
    val builder = LinkisClient.getLinkisClient.newLinkisJobBuilder()
      .addExecuteUser(linkisSessionImpl.getUserName)
      .addLabel(LabelKeyUtils.ENGINE_TYPE_LABEL_KEY, getEngineTypeWithVersion)
      .addLabel(LabelKeyUtils.USER_CREATOR_LABEL_KEY, linkisSessionImpl.getUserName + "-" + linkisSessionImpl.getCreator)
      .addJobContent("runType", linkisSessionImpl.getRunTypeStr)
      .addJobContent("code", statement)
      .addSource("sourceIpAddress", linkisSessionImpl.getIpAddress)
      .addSource("submitService", Utils.getLocalHostname)
      .addSource("sessionId", linkisSessionImpl.getSessionHandle.getHandleIdentifier.toString)
    if(confOverlay != null && confOverlay.size() > 0) {
      confOverlay.asScala.foreach{ case (k, v) => builder.addVariable(k, v)}
    }
    val linkisJob = builder.build()
    linkisJob.submit()
    this.confOverlay.put(ConfVars.HIVEQUERYID.varname, linkisJob.getId)
    sqlOpDisplay.getQueryDisplay.setQueryId(linkisJob.getId)
    this.linkisJob = linkisJob
    linkisJob.waitForCompleted()
    if(!linkisJob.isSucceed) {
      val errorMsg = linkisJob.getJobInfo.getErrCode + ": " + linkisJob.getJobInfo.getErrDesc
      sqlOpDisplay.getQueryDisplay.setErrorMessage(errorMsg)
      throw new HiveSQLException(errorMsg)
    }
    setHasResultSet(linkisJob.existResultSets)
    if(hasResultSet) {
      resetFetch()
      val fieldSchemas = resultIterator.getMetadata.asScala.map { field =>
        field.get("dataType") match {
          case "decimal" =>
            // 兼容Linkis老版本，给出默认精度
            new FieldSchema(field.get("columnName"), "decimal(10,2)", field.get("comment"))
          case "long" =>
            new FieldSchema(field.get("columnName"), "bigint", field.get("comment"))
          case dataType => new FieldSchema(field.get("columnName"), dataType, field.get("comment"))
        }
      }.toList.asJava
      this.mResultSchema = new Schema(new util.ArrayList[FieldSchema](fieldSchemas), null)
      this.resultSchema = new TableSchema(this.mResultSchema)
    }
  }

  override protected def killJob(): Unit = if(linkisJob != null && !linkisJob.isCompleted) linkisJob.kill()

  def getProgress: Double = if(linkisJob == null) 0.0 else linkisJob.getJobInfo.getProgress.toDouble

  override protected def resetFetch(): Unit = {
    val resultPath = linkisJob.getResultSetIterables(0).getFsPath
    sqlOpDisplay.getQueryDisplay.setExplainPlan(resultPath)
    resultIterator = linkisJob.getResultSetIterables(0).iterator match {
      case iterator: TableResultSetIterator => iterator
    }
  }

  override protected def fetchNext(maxRows: Int): util.List[util.List[Object]] = {
    val result = new util.ArrayList[util.List[Object]]
    var fetched = 0
    while(resultIterator.hasNext() && fetched < maxRows) {
      val row = resultIterator.next()
      result.add(row.asScala.map(_.asInstanceOf[Object]).asJava)
      fetched += 1
    }
    result
  }

  override def getTaskStatus: String = if(linkisJob != null) linkisJob.getJobInfo.getStatus else ""

  /**
   * get the execution engine, default in spark.
   * WARNING: do not try to change this method.
   * @return
   */
  override def getExecutionEngine: String = parentSession match {
    case sessionImpl: LinkisSessionImpl => sessionImpl.getEngineType
  }

  def getEngineTypeWithVersion: String = linkisSessionImpl.getEngineType + "-" + linkisSessionImpl.getEngineVersion

  override def createOperationLog(): Unit = if (this.parentSession.isOperationLogEnabled) {
    isOperationLogEnabled = true
    val operationFile = new File(this.parentSession.getOperationLogSessionDir, getHandle.getHandleIdentifier.toString)
    if (!operationFile.exists() && !operationFile.createNewFile && !operationFile.canRead && !operationFile.canWrite) {
      warn("The operation log file cannot be created, read or written: " + operationFile.getAbsolutePath)
      this.isOperationLogEnabled = false
      return
    }
    operationLog = new OperationLog(getHandle.toString, operationFile ,parentSession.getHiveConf) {
      private var jobLogs: util.ArrayList[String] = _
      private var index = 0

      override def writeOperationLog(operationLogMessage: String): Unit = {
        info(s"$operationFile => $operationLogMessage")
      }

      override def writeOperationLog(level: OperationLog.LoggingLevel, operationLogMessage: String): Unit = {
        info(s"$level $operationFile => $operationLogMessage")
      }

      override def readOperationLog(isFetchFirst: Boolean, maxRows: Long): util.List[String] = {
        if(linkisJob == null) return new util.ArrayList[String]
        else if(jobLogs == null) {
          jobLogs = new util.ArrayList[String]
          linkisJob.addLogListener(new LogListener {
            override def onLogUpdate(logs: util.List[String]): Unit = {
              jobLogs.addAll(logs)
            }
          })
        }
        if(isFetchFirst) index = 0
        val from = index
        if(jobLogs.size > maxRows + from) {
          index += maxRows.toInt
          jobLogs.subList(from, maxRows.toInt)
        } else if(jobLogs.size() <= from) {
          new util.ArrayList[String]
        } else {
          index = jobLogs.size()
          jobLogs.subList(from, jobLogs.size())
        }
      }

      override def close(): Unit = {
        if(jobLogs != null) {
          jobLogs.clear()
          jobLogs = null
        }
        super.close()
      }
    }
  }
}