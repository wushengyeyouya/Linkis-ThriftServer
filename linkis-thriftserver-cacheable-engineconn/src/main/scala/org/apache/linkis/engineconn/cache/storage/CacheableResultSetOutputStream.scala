package org.apache.linkis.engineconn.cache.storage

import java.io.OutputStream
import java.util

import org.apache.linkis.common.io.{MetaData, Record}
import org.apache.linkis.common.utils.{ByteTimeUtils, Logging, Utils}
import org.apache.linkis.engineconn.cache.exception.{ReleaseMemoryFailedErrorException, ResultSetCacheErrorException}
import org.apache.linkis.engineconn.cache.operation.ResultSetMetric.opTime
import org.apache.linkis.engineconn.cache.operation.{ResultSetMemoryManager, ResultSetMetric}
import org.apache.linkis.storage.resultset.StorageResultSetWriter

/**
  * Created by enjoyyin.
  */
class CacheableResultSetOutputStream[K <: MetaData, V <: Record](private[cache] val writer: StorageResultSetWriter[K, V],
                                                                 resultSetMetric: ResultSetMetric)
  extends OutputStream with Logging {

  private[cache] var buf: Array[Byte] = Array.empty
  private var index: Int = 0
  private var cacheOffset: Int = 0
  private var cacheRows, rowOffset = 0
  private var resultSetMemoryManager: ResultSetMemoryManager = _
  private var isAllCached = false
  private var isClosed = false
  private val writeLock = new Array[Byte](0)
  private val persistLock = new Array[Byte](0)

  def setResultSetMemoryManager(resultSetMemoryManager: ResultSetMemoryManager): Unit =
    this.resultSetMemoryManager = resultSetMemoryManager

  private def acquireMemory(len: Int): Unit = if(index + len - buf.length > 0) {
    val numBytes = if(len < this.buf.length) this.buf.length << 1 else this.buf.length + len
    val buf = opTime(Utils.tryCatch(resultSetMemoryManager.acquireMemory(this, numBytes)){
      case t: ReleaseMemoryFailedErrorException if resultSetMemoryManager.isReleaseFailedException(t) =>
        acquireMemory(len)
        return
      case t => throw t
    }) {
      acquireTime =>
        resultSetMetric.increaseCacheTime(acquireTime)
        info(s"Acquire to memory ${ByteTimeUtils.bytesToString(numBytes)} for ${writer.toFSPath.getSchemaPath} cost ${ByteTimeUtils.msDurationToString(acquireTime)}.")
    }
    resultSetMetric.setResultSetCacheBytes(numBytes)
    synchronized {
      System.arraycopy(this.buf, 0, buf, 0, index)
      this.buf = buf
    }
  }

  private def releaseMemory(numBytes: Int): Unit = {
    val releaseTo = this.buf.length - numBytes
    val buf = opTime(resultSetMemoryManager.acquireMemory(this, releaseTo)) {
      acquireTime =>
        resultSetMetric.increaseCacheTime(acquireTime)
        info(s"Release to memory ${ByteTimeUtils.bytesToString(releaseTo)} for ${writer.toFSPath.getSchemaPath} cost ${ByteTimeUtils.msDurationToString(acquireTime)}.")
    }
    synchronized {
      if(index > numBytes) System.arraycopy(this.buf, numBytes, buf, 0, index - numBytes)
      this.buf = buf
    }
  }

  def setMetaData(metaData: MetaData): Unit = opTime {
    writer.addMetaData(metaData)
    writer.flush()
  }(resultSetMetric.increaseWriteTime)

  def getBufferSize: Int = buf.length

  def getCacheSize: Int = index

  def getOffset: Long = cacheOffset

  def getCacheRows: Int = cacheRows

  def getRowOffset: Int = rowOffset

  private def ensureNotClosed[T](op: => T): T = if(isClosed) throw new ResultSetCacheErrorException(50062, s"The outputStream of fsPath ${writer.toFSPath.getSchemaPath} is closed.")
  else op

  override def write(b: Int): Unit = throw new ResultSetCacheErrorException(50062, s"Not support method.")

  override def write(b: Array[Byte], off: Int, len: Int): Unit = ensureNotClosed {
    acquireMemory(len) // 因为写操作是单线程的，所以这里可以分为两把锁来操作，用于提升并发能力
    writeLock synchronized {
      System.arraycopy(b, off, buf, index, len)
      index += len
      cacheRows += 1
    }
    persistLock synchronized persistLock.notify()
  }

  def allTryToCached(): Unit = isAllCached = true

  def isResultSetAllCached: Boolean = isAllCached

  def closed: Boolean = isClosed

  override def close(): Unit = if(!isClosed) writeLock synchronized {
    persist(false)
    opTime(writer.close())(resultSetMetric.increaseWriteTime)
    isClosed = true
    resultSetMemoryManager.releaseAllMemory(this)
  }

  /**
    * 这里区分三种情况：
    * 1. cache与client同步。即client现在要读的数据，cache里面就存在，且cache刚好能覆盖client读取的速度
    * 2. cache的速度远远快于client读取的速度。如果内存够用则不会有影响；如果内存不够用，这时会出现cache不得不释放占用的内存，
    * 这时又有两个场景：一是释放掉client已经读取过的cache就够用，则没有任何影响，二是如果已经释放掉了读取过的内存还不够用，则需要进一步对
    * 内存进行释放，这时client的一部分数据需要去底层存储层去读取。
    * 3. cache的速度慢于client读取的速度。client读取时等待cache即可
    *
    * @return
    */
  def tryRead(offset: Int): Int = ensureNotClosed {
    persistLock synchronized {
      if(offset < cacheOffset) throw new ResultSetCacheErrorException(50062, s"The request offset $offset is less than cacheOffset $cacheOffset.")
      else if(offset < cacheOffset + index && buf.nonEmpty) buf(offset - cacheOffset)
      else if(isAllCached) -1
      else {
        while(offset >= cacheOffset + index || buf.isEmpty) {
          persistLock.wait(100)
        }
        tryRead(offset)
      }
    }
  }

  def toByteArray: Array[Byte] = writeLock synchronized {
    util.Arrays.copyOf(buf, index)
  }

  def persist(): Unit = persist(true)

  def persist(isReleaseMemory: Boolean): Unit = if(index > 0) persistTo(index + cacheOffset, cacheRows + rowOffset, isReleaseMemory)
//  persistLock synchronized {
//    if(index > 0){
//      val (releaseBytes, releaseRows) = (index, cacheRows)
//      opTime {
//        writer.writeLine(buf.take(releaseBytes))
//        writer.flush()
//      }(resultSetMetric.increaseWriteTime)
//      resultSetMetric.increaseTotalBytes(index)
//      releaseMemory(releaseBytes)
//      cacheOffset += releaseBytes
//      rowOffset += releaseRows
//      index = 0
//      cacheRows = 0
//    }
//  }

  def persistTo(offset: Int, rowOffset: Int): Int = persistTo(offset, rowOffset, true)

  def persistTo(offset: Int, rowOffset: Int, isReleaseMemory: Boolean): Int = ensureNotClosed {
    if(cacheOffset < offset) persistLock synchronized {
      if(cacheOffset < offset) {
        val releaseBytes = offset - cacheOffset
        if(releaseBytes > index) throw new ResultSetCacheErrorException(50062, s"Persist bytes $releaseBytes, but index only reached $index.")
        opTime {
          writer.writeLine(buf.take(releaseBytes))
          writer.flush()
        }(resultSetMetric.increaseWriteTime)
        resultSetMetric.increaseTotalBytes(releaseBytes)
        if(isReleaseMemory) releaseMemory(releaseBytes)
        writeLock synchronized {
          index -= releaseBytes
          cacheRows -= (rowOffset - this.rowOffset)
          cacheOffset = offset
          this.rowOffset = rowOffset
        }
        releaseBytes
      } else 0
    } else 0
  }

}
