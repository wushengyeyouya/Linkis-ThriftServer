package org.apache.linkis.engineconn.cache.operation

import java.io.Closeable
import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.commons.io.IOUtils
import org.apache.linkis.common.io.resultset.ResultSet
import org.apache.linkis.common.io.{MetaData, Record}
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.engineconn.cache.storage.{CacheableResultSetInputStream, CacheableStorageResultSetWriter, ResultSetIterator}
import org.apache.linkis.storage.resultset.table.{TableMetaData, TableResultDeserializer}
import org.apache.linkis.storage.resultset.{StorageResultSetReader, StorageResultSetWriter}

/**
  * Created by enjoyyin.
  */
class ResultSetOperation[K <: MetaData, V <: Record](writer: StorageResultSetWriter[K, V],
                                                     iterator: ResultSetIterator[V],
                                                     resultSet: ResultSet[K, V])
  extends Closeable with MemoryReleaser with Logging {

  protected var maxWaitReadTime: Long = 10L
  protected var executorService: ScheduledThreadPoolExecutor = _
  private var waitTask: ScheduledFuture[_] = _
  private var isClosed = false
  private var resultSetMemoryManager: ResultSetMemoryManager = _
  private val resultSetMetric = new ResultSetMetric(writer.toFSPath)
  private val cacheableStorageResultSetWriter: CacheableStorageResultSetWriter[K, V] =
    new CacheableStorageResultSetWriter[K, V](resultSet, writer, resultSetMetric)

  def setStorageUser(storageUser: String): Unit = {
    cacheableStorageResultSetWriter.setStorageUser(storageUser)
  }

  def setMaxWaitReadTime(maxWaitReadTime: Long): Unit = this.maxWaitReadTime = maxWaitReadTime

  def setExecutorService(executorService: ScheduledThreadPoolExecutor): Unit = this.executorService = executorService

  def setResultSetMemoryManager(resultSetMemoryManager: ResultSetMemoryManager): Unit = this.resultSetMemoryManager = resultSetMemoryManager

  def init(): Unit = {
    cacheableStorageResultSetWriter.getCacheableOutputStream.setResultSetMemoryManager(resultSetMemoryManager)
    resultSetMemoryManager.registerMemoryReleaser(this)
    waitTask = executorService.schedule(new Runnable {
      override def run(): Unit = Utils.tryCatch {
        resultSetMetric.setIteratorStartTime(System.currentTimeMillis)
        logger.info(s"Job ${iterator.getName} start to cache and write all resultSets to path ${writer.toFSPath.getSchemaPath}.")
        cacheableStorageResultSetWriter.addMetaData(iterator.getMetaData)
        iterator.getMetaData match {
          case tableMetaData: TableMetaData => resultSetMetric.setColumns(tableMetaData.columns.length)
          case _ =>
        }
        while(iterator.hasNext) {
          val record = iterator.next()
          resultSetMetric.increaseRecord()
          cacheableStorageResultSetWriter.addRecord(record)
        }
        cacheableStorageResultSetWriter.getCacheableOutputStream.allTryToCached()
        resultSetMetric.setIteratorEndTime(System.currentTimeMillis)
        logger.info(s"Job ${iterator.getName} iterate resultSet to path ${writer.toFSPath.getSchemaPath} costs ${resultSetMetric.getResultSetIteratorTime}.")
        synchronized(notify())
      }{t =>
        IOUtils.closeQuietly(iterator)
        logger.error(s"Job ${iterator.getName} cache the resultSet of path ${writer.toFSPath.getSchemaPath} failed", t)
      }
    }, maxWaitReadTime, TimeUnit.MILLISECONDS)
  }

  private var storageResultSetReader: StorageResultSetReader[K, V] = _
  private var inputStream: CacheableResultSetInputStream[K, V] = _
  private var resultSetLastReadTime: Long = _

  def toNewResultSetReader: StorageResultSetReader[K, V] = {
    val resultSetReader = new StorageResultSetReader[K, V](resultSet,
      new CacheableResultSetInputStream(cacheableStorageResultSetWriter, new ResultSetMetric(writer.toFSPath)))
    initResultSetReader(resultSetReader)
    resultSetReader
  }

  def getResultSetReader: StorageResultSetReader[K, V] = {
    if(storageResultSetReader == null) synchronized {
      if(storageResultSetReader == null) {
        inputStream = new CacheableResultSetInputStream[K, V](cacheableStorageResultSetWriter, resultSetMetric)
        storageResultSetReader = new StorageResultSetReader[K, V](resultSet, inputStream) {
          override def getRecord: Record = {
            resultSetLastReadTime = System.currentTimeMillis
            resultSetMetric.increaseReadRecord()
            resultSetMetric.setReadEndTime(System.currentTimeMillis)
            super.getRecord
          }
          override def getMetaData: MetaData = {
            resultSetMetric.setReadStartTime(System.currentTimeMillis)
            iterator.getMetaData
          }
        }
      }
      initResultSetReader(storageResultSetReader)
    }
    storageResultSetReader
  }

  def getReadBytes: Int = if(inputStream != null) inputStream.getReadBytes else 0

  private def initResultSetReader(resultSetReader: StorageResultSetReader[K, V]): Unit = {
    ResultSetOperation.metaDataField.set(resultSetReader, iterator.getMetaData)
    ResultSetOperation.deserializerField.get(resultSetReader) match {
      case deserializer: TableResultDeserializer =>
        deserializer.metaData = iterator.getMetaData.asInstanceOf[TableMetaData]
    }
  }

  def getResultSetMetrics: ResultSetMetric = resultSetMetric

  def isCompleted: Boolean = isClosed
  def isResultSetIteratorCompleted: Boolean = waitTask.isDone
  def getResultSetLastReadTime: Long = resultSetLastReadTime

  override def close(): Unit = if(!isClosed) {
    logger.info(s"Try to close ResultSetOperation(${writer.toFSPath.getSchemaPath}) in job ${iterator.getName}.")
    if(!waitTask.isDone) synchronized {
      while(!waitTask.isDone) wait(1000)
    }
    Utils.tryFinally {
      cacheableStorageResultSetWriter.close()
      if(storageResultSetReader != null) storageResultSetReader.close()
    }{
      resultSetMemoryManager.unregisterMemoryReleaser(this)
      isClosed = true
    }
    logger.info(s"ResultSetOperation(${writer.toFSPath.getSchemaPath}) in job ${iterator.getName} closed. The Metric information is " + resultSetMetric.getMetrics)
  }

  override def tryRelease(numBytes: Int): Int = if(storageResultSetReader == null) 0
    else cacheableStorageResultSetWriter.getCacheableOutputStream
      .persistTo(inputStream.getReadBytes, storageResultSetReader.getPosition.toInt)

}
object ResultSetOperation {
  private val metaDataField = classOf[StorageResultSetReader[_, _]].getDeclaredField("metaData")
  metaDataField.setAccessible(true)
  private val deserializerField = classOf[StorageResultSetReader[_, _]].getDeclaredField("deserializer")
  deserializerField.setAccessible(true)
}