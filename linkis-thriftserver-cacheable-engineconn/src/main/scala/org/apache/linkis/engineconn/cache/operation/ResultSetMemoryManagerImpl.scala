package org.apache.linkis.engineconn.cache.operation

import java.util

import org.apache.linkis.common.io.{FsPath, MetaData, Record}
import org.apache.linkis.common.utils.{ByteTimeUtils, Logging}
import org.apache.linkis.engineconn.cache.conf.ResultSetCacheConfiguration._
import org.apache.linkis.engineconn.cache.exception.ReleaseMemoryFailedErrorException
import org.apache.linkis.engineconn.cache.storage.CacheableResultSetOutputStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
class ResultSetMemoryManagerImpl private[cache]() extends ResultSetMemoryManager with Logging {

  private val taskToFsPaths = new util.HashMap[String, util.Set[String]]()
  private val fsPathToCacheOutputStreams = new util.HashMap[String, CacheableResultSetOutputStream[_ <: MetaData, _ <: Record]]()
  private val cachedByteArrays = ArrayBuffer[ArrayBlock]()
  private val maxByteArraySize = 1 << 30
  private val registeredMemoryReleasers = new util.LinkedList[MemoryReleaser]


  override def registerMemoryReleaser(memoryReleaser: MemoryReleaser): Unit = registeredMemoryReleasers.add(memoryReleaser)

  override def unregisterMemoryReleaser(memoryReleaser: MemoryReleaser): Unit = synchronized {
    registeredMemoryReleasers.remove(memoryReleaser)
  }

  override def getMemoryUsed: Long = cachedByteArrays.map(_.array.length).sum

  override def getTaskUsedMemory(fsPath: FsPath): Long = taskToFsPaths.get(fsPath.getSchemaPath)
    .asScala.map(fsPath => fsPathToCacheOutputStreams.get(fsPath).getBufferSize).sum

  override def acquireMemory(cacheableResultSetOutputStream: CacheableResultSetOutputStream[_ <: MetaData, _ <: Record],
                             numBytes: Long): Array[Byte] = synchronized {
    val fsPath = cacheableResultSetOutputStream.writer.toFSPath
    val fsSchemaPath = fsPath.getSchemaPath
    val parent = fsPath.getParent.getSchemaPath
    if(!taskToFsPaths.containsKey(parent)) {
      taskToFsPaths.put(parent, new util.HashSet[String])
      taskToFsPaths.get(parent).add(fsSchemaPath)
    }
    val usedMemory = if(!fsPathToCacheOutputStreams.containsKey(fsSchemaPath)) 0
    else fsPathToCacheOutputStreams.get(fsSchemaPath).getBufferSize
    val tryAllocate = capacity(numBytes.toInt)
    info(s"ResultSet path $fsSchemaPath usedMemory: $usedMemory, acquire numBytes: $numBytes, actual allocate numBytes: $tryAllocate.")
    def releaseLast(): Unit = if(fsPathToCacheOutputStreams.containsKey(fsSchemaPath))
      cachedByteArrays.find(_.array == cacheableResultSetOutputStream.buf).foreach(_.free())
    else fsPathToCacheOutputStreams.put(fsSchemaPath, cacheableResultSetOutputStream)
    findFree(tryAllocate).map{ buf =>
      releaseLast()
      buf
    } getOrElse {
      val tryRelease = getMemoryUsed + tryAllocate - usedMemory - getMaxUsefulMemory
      if(usedMemory < tryAllocate && tryRelease > 0) {
        info(s"Try to release ${ByteTimeUtils.bytesToString(tryRelease)} for resultSet path $fsSchemaPath.")
        val startTime = System.currentTimeMillis
        var released, tryNum = 0
        while(tryRelease > released && tryNum < 3) {
          // try to release memory
          cachedByteArrays.filter(_.canRelease).toArray.foreach(cachedByteArrays -= _)
          // 已经返回给前端的，都可以释放掉
          registeredMemoryReleasers.asScala.foreach(released += _.tryRelease(tryAllocate))
          tryNum += 1
        }
        if(tryRelease > released) throw new ReleaseMemoryFailedErrorException(50068, "Release memory failed!")
        info(s"Release memory for resultSet path $fsSchemaPath cost ${ByteTimeUtils.msDurationToString(System.currentTimeMillis - startTime)}.")
      }
      releaseLast()
      findFree(tryAllocate).getOrElse {
        val arrayBlock = ArrayBlock(tryAllocate)
        cachedByteArrays += arrayBlock
        arrayBlock.increaseUsage()
      }
    }
  }

  private def findFree(numBytes: Int): Option[Array[Byte]] = {
    val block = cachedByteArrays.find(block => block.isFree && block.array.length == numBytes) orElse cachedByteArrays.find{ block =>
      block.isFree && block.array.length / numBytes == 2}
    block.map(_.increaseUsage())
  }

  override def releaseAllMemory(cacheableResultSetOutputStream: CacheableResultSetOutputStream[_ <: MetaData, _ <: Record]): Unit = synchronized {
    val fsPath = cacheableResultSetOutputStream.writer.toFSPath
    releaseResultSetMemory(fsPath)
  }

  def releaseResultSetMemory(fsPath: FsPath): Unit = synchronized {
    val parent = fsPath.getParent.getSchemaPath
    val fsSchemaPath = fsPath.getSchemaPath
    assert(taskToFsPaths.containsKey(parent))
    cachedByteArrays.find(_.array == fsPathToCacheOutputStreams.get(fsSchemaPath).buf).foreach(_.free())
    fsPathToCacheOutputStreams.remove(fsSchemaPath)
    taskToFsPaths.get(parent).remove(fsSchemaPath)
    if(taskToFsPaths.get(parent).isEmpty) taskToFsPaths.remove(parent)
  }

  override def releaseTaskMemory(fsPath: FsPath): Unit = synchronized {
    val fsSchemaPath = fsPath.getSchemaPath
    taskToFsPaths.get(fsSchemaPath).asScala.foreach(path => releaseResultSetMemory(new FsPath(path)))
  }


  override def getMetrics: Map[String, Any] = Map(
    "TotalCacheMemory" -> ByteTimeUtils.bytesToString(getMaxUsefulMemory),
    "memoryUsed" -> ByteTimeUtils.bytesToString(getMemoryUsed),
    "memoryLeft" -> ByteTimeUtils.bytesToString(getMemoryLeft))

  override def isReleaseFailedException(exception: ReleaseMemoryFailedErrorException): Boolean = exception.getErrCode == 50068

  case class ArrayBlock(array: Array[Byte]) {
    var useNum: Int = 0
    var lastUsedTime: Long = System.currentTimeMillis
    val createTime: Long = System.currentTimeMillis
    var isFree = true
    def getUseNum: Int = useNum
    def increaseUsage(): Array[Byte] = {
      useNum += 1
      isFree = false
      lastUsedTime = System.currentTimeMillis
      array
    }
    def free(): Unit = {
      isFree = true
      lastUsedTime = System.currentTimeMillis
    }
    def canRelease: Boolean = isFree && (System.currentTimeMillis - lastUsedTime > ENGINE_RESULT_SET_CACHE_BLOCK_FREE_TIME.getValue.toLong ||
      (System.currentTimeMillis - createTime) / useNum > ENGINE_RESULT_SET_CACHE_BLOCK_FREE_TIME.getValue.toLong / 2)
  }
  object ArrayBlock {
    def apply(numBytes: Long): ArrayBlock = new ArrayBlock(new Array[Byte](numBytes.toInt))
  }

  private def capacity(numBytes: Int): Int = {
    var n = numBytes - 1
    n |= n >>> 1
    n |= n >>> 2
    n |= n >>> 4
    n |= n >>> 8
    n |= n >>> 16
    if (n < 0) 1
    else if (n >= maxByteArraySize) maxByteArraySize
    else n + 1
  }
}

