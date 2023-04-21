package org.apache.linkis.engineconn.cache.operation

import java.io.Closeable
import java.util.concurrent.ScheduledThreadPoolExecutor

import org.apache.linkis.common.io.{FsPath, MetaData, Record}

/**
  * Created by enjoyyin.
  */
trait ResultSetOperationManager extends Closeable {

  def getResultSetMemoryManager: ResultSetMemoryManager

  def setExecutorService(executorService: ScheduledThreadPoolExecutor): Unit

  def getResultSetOperation[K <: MetaData, V <: Record](fsPath: FsPath): ResultSetOperation[K, V]

  def init(): Unit

  def getResultSetList(parentFsPath: FsPath): Array[FsPath]

  def getResultSetOperations[K <: MetaData, V <: Record](parentFsPath: FsPath): Array[ResultSetOperation[K, V]]

  def addResultSetOperation[K <: MetaData, V <: Record](fsPath: FsPath, resultSetOperation: ResultSetOperation[K, V]): Unit

  /**
    * Use another thread to close ResultSetOperation, so we can save some time to response client quickly.
    * @param resultSetOperation the resultSetOperation wait for closing
    * @tparam K TableMetaData
    * @tparam V TableRecord
    */
  def closeResultSetOperation[K <: MetaData, V <: Record](resultSetOperation: ResultSetOperation[K, V]): Unit

  def closeResultSetOperation(fsPath: FsPath): Unit

}