package org.apache.linkis.engineconn.cache.operation

import org.apache.linkis.common.io.{FsPath, MetaData, Record}
import org.apache.linkis.engineconn.cache.exception.ReleaseMemoryFailedErrorException
import org.apache.linkis.engineconn.cache.storage.CacheableResultSetOutputStream

/**
  * Created by enjoyyin.
  */
abstract class ResultSetMemoryManager {

  private var memorySize: Long = _

  def registerMemoryReleaser(memoryReleaser: MemoryReleaser): Unit

  def unregisterMemoryReleaser(memoryReleaser: MemoryReleaser): Unit

  def setMaxUsefulMemory(memorySize: Long): Unit = this.memorySize = memorySize

  def getMaxUsefulMemory: Long = memorySize

  def getMemoryUsed: Long

  def getMemoryLeft: Long = memorySize - getMemoryUsed

  def getTaskUsedMemory(fsPath: FsPath): Long

  def acquireMemory(cacheableResultSetOutputStream: CacheableResultSetOutputStream[_ <: MetaData, _ <: Record],
                    numBytes: Long): Array[Byte]

  def releaseAllMemory(cacheableResultSetOutputStream: CacheableResultSetOutputStream[_ <: MetaData, _ <: Record]): Unit

  def releaseTaskMemory(fsPath: FsPath): Unit

  def getMetrics: Map[String, Any]

  def isReleaseFailedException(exception: ReleaseMemoryFailedErrorException): Boolean

}