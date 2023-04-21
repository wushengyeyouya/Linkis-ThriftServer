package org.apache.linkis.engineconn.cache.spark.executor

import java.lang.reflect.InvocationTargetException
import java.util
import java.util.Locale

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.linkis.common.utils.Utils
import org.apache.linkis.engineconn.cache.ResultSetCacheFactory
import org.apache.linkis.engineconn.computation.executor.execute.EngineExecutionContext
import org.apache.linkis.engineplugin.spark.config.SparkConfiguration
import org.apache.linkis.engineplugin.spark.entity.SparkEngineSession
import org.apache.linkis.engineplugin.spark.exception.SparkEngineException
import org.apache.linkis.engineplugin.spark.executor.{SparkEngineConnExecutor, SparkSqlExecutor}
import org.apache.linkis.engineplugin.spark.utils.EngineUtils
import org.apache.linkis.scheduler.exception.LinkisJobRetryException
import org.apache.linkis.scheduler.executer.{ErrorExecuteResponse, ExecuteResponse, SuccessExecuteResponse}
import org.apache.linkis.storage.domain.{Column, DataType}
import org.apache.linkis.storage.resultset.ResultSetFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class SparkSqlCacheableExecutor(sparkEngineSession: SparkEngineSession, id: Long)
  extends SparkSqlExecutor(sparkEngineSession, id) {

  protected override def runCode(executor: SparkEngineConnExecutor, code: String,
                                 engineExecutionContext: EngineExecutionContext, jobGroup: String): ExecuteResponse = {
    logger.info("SQLExecutor run query: " + code)
    engineExecutionContext.appendStdout(s"${EngineUtils.getName} >> $code")
    val standInClassLoader = Thread.currentThread().getContextClassLoader
    try {
      val sqlStartTime = System.currentTimeMillis()
      Thread
        .currentThread()
        .setContextClassLoader(sparkEngineSession.sparkSession.sharedState.jarClassLoader)
      val extensions =
        org.apache.linkis.engineplugin.spark.extension.SparkSqlExtension.getSparkSqlExtensions()
      val df = sparkEngineSession.sqlContext.sql(code)

      Utils.tryQuietly(
        extensions.foreach(
          _.afterExecutingSQL(
            sparkEngineSession.sqlContext,
            code,
            df,
            SparkConfiguration.SQL_EXTENSION_TIMEOUT.getValue,
            sqlStartTime
          )
        )
      )
      showDF(
        sparkEngineSession.sparkContext,
        jobGroup,
        df,
        SparkConfiguration.SHOW_DF_MAX_RES.getValue,
        engineExecutionContext
      )
      SuccessExecuteResponse()
    } catch {
      case e: InvocationTargetException =>
        var cause = ExceptionUtils.getRootCause(e)
        if (cause == null) cause = e
        ErrorExecuteResponse(ExceptionUtils.getRootCauseMessage(e), cause)
      case ite: Exception =>
        ErrorExecuteResponse(ExceptionUtils.getRootCauseMessage(ite), ite)
    } finally {
      Thread.currentThread().setContextClassLoader(standInClassLoader)
    }
  }

  protected override def getExecutorIdPreFix: String = "SparkSqlCacheableExecutor_"

  protected def showDF(sc: SparkContext,
                       jobGroup: String,
                       dataFrame: DataFrame,
                       maxResult: Int,
                       engineExecutionContext: EngineExecutionContext): Unit ={
    if (sc.isStopped) {
      logger.error("Spark application has already stopped in showDF, please restart it.")
      throw new LinkisJobRetryException(
        "Spark application sc has already stopped, please restart it."
      )
    }
    val startTime = System.currentTimeMillis()
    val iterator = Utils.tryThrow(dataFrame.toLocalIterator) { t =>
      throw new SparkEngineException(
        DATAFRAME_EXCEPTION.getErrorCode,
        DATAFRAME_EXCEPTION.getErrorDesc,
        t
      )
    }
    val colSet = new util.HashSet[String]()
    val schema = dataFrame.schema
    var columnsSet: StructType = null
    schema foreach (s => colSet.add(s.name))
    if (colSet.size() < schema.size) {
      val arr: ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
      val tmpSet = new util.HashSet[StructField]()
      val tmpArr = new ArrayBuffer[StructField]()
      dataFrame.queryExecution.analyzed.output foreach { attri =>
        val tempAttri =
          StructField(attri.qualifiedName, attri.dataType, attri.nullable, attri.metadata)
        tmpSet.add(tempAttri)
        tmpArr += tempAttri
      }
      if (tmpSet.size() < schema.size) {
        dataFrame.queryExecution.analyzed.output foreach { attri =>
          val tempAttri =
            StructField(attri.toString(), attri.dataType, attri.nullable, attri.metadata)
          arr += tempAttri
        }
      } else {
        tmpArr.foreach(arr += _)
      }
      columnsSet = StructType(arr.toArray)
    } else {
      columnsSet = schema
    }
    val columns = columnsSet
      .map(c =>
        Column(
          c.name,
          DataType.toDataType(c.dataType.typeName.toLowerCase(Locale.getDefault())),
          c.getComment().orNull
        )
      )
      .toArray[Column]
    logger.info(s"Job $jobGroup is submitted with resultSet metadata: ${columns.toList}.")
    if (columns == null || columns.isEmpty) return
    val tableIterator = new SparkResultSetIterator(columns, maxResult, startTime, jobGroup, iterator, engineExecutionContext)
    ResultSetCacheFactory.createResultSetOperation(engineExecutionContext, tableIterator, ResultSetFactory.TABLE_TYPE)
  }

}
