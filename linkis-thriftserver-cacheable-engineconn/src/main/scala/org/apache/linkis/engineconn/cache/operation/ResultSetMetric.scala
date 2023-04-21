package org.apache.linkis.engineconn.cache.operation

import org.apache.linkis.common.io.FsPath
import org.apache.linkis.common.utils.ByteTimeUtils
import org.apache.linkis.engineconn.cache.exception.ResultSetCacheErrorException

/**
  * Created by enjoyyin.
  */
class ResultSetMetric(fsPath: FsPath) {

  private var totalBytes: Long = 0l
  private var totalRecords: Int = 0
  private var totalColumns: Int = 0

  private var resultSetIteratorTime: String = _
  private var iteratorStartTime: Long = 0l
  private var iteratorEndTime: Long = 0l

  private var resultSetCacheTime: Long = 0l

  private var resultSetReadTime: String = _
  private var readStartTime: Long = 0l
  private var readEndTime: Long = 0l

  private var resultSetReadRecords: Int = 0

  private var resultSetCacheHitBytes: Long = 0
  private var resultSetWriteTime: Long = 0
  private var resultSetMaxCacheBytes: Long = 0

  def getFsPath: FsPath = fsPath

  def increaseTotalBytes(numBytes: Long): Unit = totalBytes += numBytes

  def increaseRecord(): Unit = totalRecords += 1

  def setColumns(columnNum: Int): Unit = totalColumns = columnNum

  def setIteratorStartTime(startTime: Long): Unit = iteratorStartTime = startTime

  def setIteratorEndTime(endTime: Long): Unit = if(endTime < iteratorStartTime)
    throw new ResultSetCacheErrorException(30001, "IteratorEndTime must bigger than IteratorStartTime.")
  else iteratorEndTime = endTime

  def getResultSetIteratorTime: String = {
    if(resultSetIteratorTime == null && iteratorEndTime > 0)
      resultSetIteratorTime = ByteTimeUtils.msDurationToString(iteratorEndTime - iteratorStartTime)
    resultSetIteratorTime
  }

  def increaseCacheTime(interval: Long): Unit = resultSetCacheTime += interval

  def setReadStartTime(startTime: Long): Unit = readStartTime = startTime

  def setReadEndTime(endTime: Long): Unit = if(endTime < readStartTime)
    throw new ResultSetCacheErrorException(30001, "ReadEndTime must bigger than ReadStartTime.")
  else readEndTime = endTime

  def increaseReadRecord(): Unit = resultSetReadRecords += 1

  def getResultSetReadRecords: Int = resultSetReadRecords

  def increaseHitByte(): Unit = resultSetCacheHitBytes += 1

  def increaseWriteTime(interval: Long): Unit = resultSetWriteTime += interval

  def setResultSetCacheBytes(cacheBytes: Long): Unit = if(cacheBytes > resultSetMaxCacheBytes) resultSetMaxCacheBytes = cacheBytes

  def getMetrics: Map[String, Any] = {
    getResultSetIteratorTime
    if(resultSetReadTime == null && readEndTime > 0) resultSetReadTime = ByteTimeUtils.msDurationToString(readEndTime - readStartTime)
    Map("fsPath" -> fsPath.getSchemaPath, "totalBytes" -> totalBytes, "totalRecords" -> totalRecords, "totalColumns" -> totalColumns,
    "resultSetIteratorTime" -> resultSetIteratorTime, "resultSetCacheTime" -> resultSetCacheTime, "resultSetReadTime" -> resultSetReadTime,
    "resultSetReadRecords" -> resultSetReadRecords, "resultSetCacheHitBytes" -> resultSetCacheHitBytes, "resultSetWriteTime" -> resultSetWriteTime, "resultSetMaxCacheBytes" -> resultSetMaxCacheBytes)
  }

}
object ResultSetMetric {

  def opTime[T](op: => T)(increaseTo: Long => Unit): T = {
    val startTime = System.currentTimeMillis
    val result = op
    val costTime = System.currentTimeMillis - startTime
    increaseTo(costTime)
    result
  }

  def opTimeResult[T](op: => T)(increaseTo: (T, Long) => Unit): T = {
    val startTime = System.currentTimeMillis
    val result = op
    val costTime = System.currentTimeMillis - startTime
    increaseTo(result, costTime)
    result
  }

}