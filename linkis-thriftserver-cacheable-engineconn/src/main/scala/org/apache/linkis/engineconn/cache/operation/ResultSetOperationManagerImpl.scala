package org.apache.linkis.engineconn.cache.operation

import java.util
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import org.apache.commons.io.IOUtils
import org.apache.linkis.common.io.{FsPath, MetaData, Record}
import org.apache.linkis.common.utils.{ByteTimeUtils, Logging, Utils}
import org.apache.linkis.engineconn.cache.conf.ResultSetCacheConfiguration._

import scala.collection.JavaConverters._

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
class ResultSetOperationManagerImpl extends ResultSetOperationManager with Logging {

  private val taskToFsPaths = new util.HashMap[String, util.Set[String]]()
  private val fsPathToResultSetOperations = new util.HashMap[String, ResultSetOperation[_ <: MetaData, _ <: Record]]()
  protected var executorService: ScheduledThreadPoolExecutor = _
  private val resultSetMemoryManager = createResultSetMemoryManager()

  protected def createResultSetMemoryManager(): ResultSetMemoryManager = new ResultSetMemoryManagerImpl

  override def getResultSetMemoryManager: ResultSetMemoryManager = resultSetMemoryManager

  override def setExecutorService(executorService: ScheduledThreadPoolExecutor): Unit = this.executorService = executorService

  override def getResultSetOperation[K <: MetaData, V <: Record](fsPath: FsPath): ResultSetOperation[K, V] = {
    val path = fsPath.getSchemaPath
    fsPathToResultSetOperations.get(path).asInstanceOf[ResultSetOperation[K, V]]
  }

  override def init(): Unit = {
    val resultSetCacheMaxMemory = (Runtime.getRuntime.totalMemory * ENGINE_RESULT_SET_CACHE_MEMORY_SIZE.acquireNew).toInt
    resultSetMemoryManager.setMaxUsefulMemory(resultSetCacheMaxMemory)
    info(s"The max memory of this JVM is ${ByteTimeUtils.bytesToString(Runtime.getRuntime.totalMemory)}, Set max resultSet cache memory to ${ByteTimeUtils.bytesToString(resultSetCacheMaxMemory)}.")
    executorService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        info("Try to scan and close completed resultSetOperation. resultSetMemory metric: " + resultSetMemoryManager.getMetrics)
        fsPathToResultSetOperations.values().asScala.filter(canClose).foreach(IOUtils.closeQuietly)
        val removeOperations = fsPathToResultSetOperations.asScala.filter(_._2.isCompleted).keysIterator.toArray
        if(removeOperations.nonEmpty) synchronized (util.Arrays.copyOf(removeOperations, removeOperations.length).foreach {
          fsPath =>
            fsPathToResultSetOperations.remove(fsPath)
            val parent = new FsPath(fsPath).getParent.getSchemaPath
            taskToFsPaths.get(parent).remove(fsPath)
            if(taskToFsPaths.get(parent).isEmpty) taskToFsPaths.remove(parent)
        })
        info("Scan task for completed resultSetOperation is done. resultSetMemory metric: " + resultSetMemoryManager.getMetrics)
      }
    }, ENGINE_RESULT_SET_CACHE_EXISTS_TIME.getValue.toLong,
      ENGINE_RESULT_SET_CACHE_EXISTS_TIME.getValue.toLong, TimeUnit.MILLISECONDS)
  }

  def canClose(resultSetOperation: ResultSetOperation[_ <: MetaData, _ <: Record]): Boolean = {
    !resultSetOperation.isCompleted && resultSetOperation.isResultSetIteratorCompleted &&
      System.currentTimeMillis - resultSetOperation.getResultSetLastReadTime > ENGINE_RESULT_SET_CACHE_EXISTS_TIME.getValue.toLong
  }

  override def getResultSetList(parentFsPath: FsPath): Array[FsPath] = taskToFsPaths.get(parentFsPath.getSchemaPath).asScala.map(new FsPath(_)).toArray

  override def getResultSetOperations[K <: MetaData, V <: Record](parentFsPath: FsPath): Array[ResultSetOperation[K, V]] = {
    taskToFsPaths.get(parentFsPath.getSchemaPath).asScala.map(fsPathToResultSetOperations.get(_).asInstanceOf[ResultSetOperation[K, V]]).toArray
  }

  override def addResultSetOperation[K <: MetaData, V <: Record](fsPath: FsPath,
                                                                 resultSetOperation: ResultSetOperation[K, V]): Unit = synchronized {
    val parentFsPath = fsPath.getParent.getSchemaPath
    if(!taskToFsPaths.containsKey(parentFsPath)) synchronized {
      if(!taskToFsPaths.containsKey(parentFsPath)) taskToFsPaths.put(parentFsPath, new util.HashSet[String])
    }
    taskToFsPaths.get(parentFsPath).add(fsPath.getSchemaPath)
    fsPathToResultSetOperations.put(fsPath.getSchemaPath, resultSetOperation)
    resultSetOperation.setExecutorService(executorService)
    resultSetOperation.setMaxWaitReadTime(ENGINE_RESULT_SET_CACHE_READ_WAIT_TIME.getValue.toLong)
    resultSetOperation.setResultSetMemoryManager(resultSetMemoryManager)
    resultSetOperation.init()
  }

  override def closeResultSetOperation[K <: MetaData, V <: Record](resultSetOperation: ResultSetOperation[K, V]): Unit = {
    executorService.submit(new Runnable {
      override def run(): Unit = Utils.tryAndWarn(resultSetOperation.close())
    })
  }

  override def closeResultSetOperation(fsPath: FsPath): Unit = closeResultSetOperation(getResultSetOperation(fsPath))

  override def close(): Unit = {
    fsPathToResultSetOperations.values().asScala.foreach(IOUtils.closeQuietly)
    executorService.shutdown()
  }
}