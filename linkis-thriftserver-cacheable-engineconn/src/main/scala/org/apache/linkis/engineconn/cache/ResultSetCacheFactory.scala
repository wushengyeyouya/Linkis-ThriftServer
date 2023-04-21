package org.apache.linkis.engineconn.cache

import org.apache.commons.lang3.StringUtils
import org.apache.linkis.DataWorkCloudApplication
import org.apache.linkis.common.io.resultset.ResultSet
import org.apache.linkis.common.io.{MetaData, Record}
import org.apache.linkis.engineconn.cache.operation.{ResultSetOperation, ResultSetOperationManager}
import org.apache.linkis.engineconn.cache.storage.ResultSetIterator
import org.apache.linkis.engineconn.computation.executor.execute.EngineExecutionContext
import org.apache.linkis.storage.resultset.{ResultSetFactory, StorageResultSetWriter}

/**
  * Created by enjoyyin on 2020/10/12.
  */
object ResultSetCacheFactory {

  private var resultSetOperationManager: ResultSetOperationManager = _

  def getResultSetOperationManager: ResultSetOperationManager = {
    if(resultSetOperationManager == null)
      resultSetOperationManager = DataWorkCloudApplication.getApplicationContext.getBean(classOf[ResultSetOperationManager])
    resultSetOperationManager
  }

  def createResultSetOperation[K <: MetaData, V <: Record](engineExecutorContext: EngineExecutionContext,
                                                           iterator: ResultSetIterator[V],
                                                           resultSet: ResultSet[K, V], alias: String): ResultSetOperation[K, V] = {
    val writer = if(StringUtils.isNotBlank(alias)) engineExecutorContext.createResultSetWriter(resultSet, alias).asInstanceOf[StorageResultSetWriter[K, V]]
    else engineExecutorContext.createResultSetWriter(resultSet)
    val resultSetOperation = writer match {
      case w: StorageResultSetWriter[K, V] => new ResultSetOperation[K, V](w, iterator, resultSet)
    }
    getResultSetOperationManager.addResultSetOperation(writer.toFSPath, resultSetOperation)
    resultSetOperation
  }

  def createResultSetOperation[K <: MetaData, V <: Record](engineExecutorContext: EngineExecutionContext,
                                                           iterator: ResultSetIterator[V],
                                                           resultSetType: String): ResultSetOperation[K, V] = {
    createResultSetOperation(engineExecutorContext, iterator, resultSetType, null)
  }

  def createResultSetOperation[K <: MetaData, V <: Record](engineExecutorContext: EngineExecutionContext,
                                                           iterator: ResultSetIterator[V],
                                                           resultSetType: String, alias: String): ResultSetOperation[K, V] = {
    val resultSet = ResultSetFactory.getInstance.getResultSetByType(resultSetType)
    createResultSetOperation(engineExecutorContext, iterator, resultSet.asInstanceOf[ResultSet[K, V]], alias)
  }

}