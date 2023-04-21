package org.apache.linkis.engineconn.cache.storage

import org.apache.linkis.common.io.resultset.{ResultSerializer, ResultSet, ResultSetWriter}
import org.apache.linkis.common.io.{FsPath, MetaData, Record}
import org.apache.linkis.engineconn.cache.operation.ResultSetMetric
import org.apache.linkis.storage.domain.Dolphin
import org.apache.linkis.storage.resultset.StorageResultSetWriter
import org.apache.linkis.storage.utils.StorageUtils

/**
  * Created by enjoyyin.
  */
class CacheableStorageResultSetWriter[K <: MetaData, V <: Record](val resultSet: ResultSet[K, V],
                                                                  writer: StorageResultSetWriter[K, V],
                                                                  resultSetMetric: ResultSetMetric)
  extends ResultSetWriter[K, V](resultSet, -1, null) {

  private val serializer: ResultSerializer = resultSet.createResultSetSerializer()
  private val outputStream = new CacheableResultSetOutputStream[K, V](writer, resultSetMetric)
  private var storageUser: String = StorageUtils.getJvmUser

  def setStorageUser(storageUser: String): Unit = {
    this.storageUser = storageUser
    outputStream.writer.setProxyUser(storageUser)
  }

  def getStorageUser: String = storageUser

  private[cache] def getCacheableOutputStream: CacheableResultSetOutputStream[K, V] = outputStream

  override def toFSPath: FsPath = writer.toFSPath

  override def addMetaDataAndRecordString(content: String): Unit = {
    val bytes = content.getBytes(Dolphin.CHAR_SET)
    outputStream.write(bytes)
  }

  override def addRecordString(content: String): Unit = {}

  override def addMetaData(metaData: MetaData): Unit = {
    outputStream.setMetaData(metaData)
  }

  override def addRecord(record: Record): Unit = {
    outputStream.write(serializer.recordToBytes(record))
  }

  override def close(): Unit = {
    outputStream.close()
  }

  override def flush(): Unit = {}

  override def toString: String = writer.toFSPath.getSchemaPath
}
