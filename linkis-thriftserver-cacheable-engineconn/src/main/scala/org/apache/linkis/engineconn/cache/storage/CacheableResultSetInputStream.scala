package org.apache.linkis.engineconn.cache.storage

import java.io.{IOException, InputStream}

import org.apache.linkis.common.io.{Fs, MetaData, Record}
import org.apache.linkis.common.utils.Utils
import org.apache.linkis.engineconn.cache.exception.ResultSetCacheErrorException
import org.apache.linkis.engineconn.cache.operation.ResultSetMetric
import org.apache.linkis.storage.FSFactory
import org.apache.linkis.storage.resultset.StorageResultSetReader

/**
  * Created by enjoyyin.
  */
class CacheableResultSetInputStream[K <: MetaData, V <: Record](cacheableStorageResultSetWriter: CacheableStorageResultSetWriter[K, V],
                                                                resultSetMetric: ResultSetMetric)
  extends InputStream {

  private val outputStream = cacheableStorageResultSetWriter.getCacheableOutputStream

  private var storageInputStream: InputStream = _
  private var storageReadBytes: Int = 0
  private var readBytes: Int = 0
  private var fs: Fs = _

  def getReadBytes: Int = readBytes

  private def ensureInputStream(): Unit = if(storageInputStream == null) {
    fs = FSFactory.getFsByProxyUser(cacheableStorageResultSetWriter.toFSPath, cacheableStorageResultSetWriter.getStorageUser)
    fs.init(null)
    storageInputStream = fs.read(cacheableStorageResultSetWriter.toFSPath)
    val resultSetReader = new StorageResultSetReader[K, V](cacheableStorageResultSetWriter.resultSet, storageInputStream)
    resultSetReader.getMetaData
  }

  override def read(): Int = if(available < 1) -1 else synchronized {
    Utils.tryCatch{
      val char = outputStream.tryRead(readBytes)
      readBytes += 1
      resultSetMetric.increaseHitByte()
      char
    } { case _: ResultSetCacheErrorException =>
        ensureInputStream()
        if(readBytes > storageReadBytes) {
          storageInputStream.skip(readBytes - storageReadBytes)
          storageReadBytes = readBytes
        }
        val char = storageInputStream.read()
        readBytes += 1
        storageReadBytes += 1
        char
      case t => throw t
    }
  }

  /**
    * Number of unread bytes remaining(剩余未读bytes数)
    *
    * @return Number of unread bytes remaining(剩余未读bytes数)
    * @throws IOException If the acquisition fails, an exception is thrown.(如获取失败，则抛出异常)
    */
  override def available: Int = if(outputStream.closed) 0
    else if(outputStream.isResultSetAllCached && readBytes == outputStream.getOffset + outputStream.getCacheSize) 0
    else 1 //这里仅仅用于协助StorageUtils的readBytes的判断

  override def close(): Unit = if(storageInputStream != null) {
    storageInputStream.close()
    fs.close()
  }
}
