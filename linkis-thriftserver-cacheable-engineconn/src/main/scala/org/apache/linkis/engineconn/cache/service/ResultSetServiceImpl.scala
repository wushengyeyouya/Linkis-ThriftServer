package org.apache.linkis.engineconn.cache.service

import java.util

import org.apache.linkis.common.io.resultset.ResultSetReader
import org.apache.linkis.common.io.{FsPath, MetaData, Record}
import org.apache.linkis.common.utils.Logging
import org.apache.linkis.engineconn.cache.conf.ResultSetCacheConfiguration._
import org.apache.linkis.engineconn.cache.exception.ResultSetCacheErrorException
import org.apache.linkis.engineconn.cache.operation.{ResultSetMetric, ResultSetOperationManager}
import org.apache.linkis.engineconn.cache.protocol._
import org.apache.linkis.storage.resultset.table.{TableMetaData, TableRecord}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by enjoyyin.
  */
class ResultSetServiceImpl(resultSetOperationManager: ResultSetOperationManager) extends AbstractResultSetService with Logging {

  private def getRecords(protocol: ResultSetRequestProtocol): ResultSetRecords = {
    val resultSetOperation = resultSetOperationManager.getResultSetOperation(new FsPath(protocol.fsPath))
    val resultSetReader = resultSetOperation.getResultSetReader
    val readBytesPosition = resultSetOperation.getReadBytes
    val records = new ArrayBuffer[Array[Any]]
    val pageSize = if(protocol.pageSize < 2 || protocol.pageSize > ENGINE_RESULT_SET_PAGE_SIZE_MAX.getValue) ENGINE_RESULT_SET_PAGE_SIZE_MAX.getValue
      else protocol.pageSize
    var hasNext = true
    var readBytes = 0
    var readTime = 0L
    ResultSetMetric.opTime {
      if(resultSetOperation.getResultSetMetrics.getResultSetReadRecords > 0) records += resultSetReader.getRecord.asInstanceOf[TableRecord].row
      hasNext = resultSetReader.hasNext
      while(records.size < pageSize && hasNext) {
        records += resultSetReader.getRecord.asInstanceOf[TableRecord].row
        hasNext = resultSetReader.hasNext
      }
      readBytes = resultSetOperation.getReadBytes - readBytesPosition
    } (readTime = _)
    if(!hasNext) resultSetOperationManager.closeResultSetOperation(resultSetOperation)
    ResultSetRecords(records.toArray, hasNext, readBytes, readTime)
//    time => info(s"Costs ${ByteTimeUtils.msDurationToString(time)} to iterator resultSet ${protocol.fsPath} for $readBytes bytes.")
  }

  private def getResultSetReader(fsPath: String): ResultSetReader[_ <: MetaData, _ <: Record] = {
    val resultSetOperation = resultSetOperationManager.getResultSetOperation(new FsPath(fsPath))
    resultSetOperation.getResultSetReader
  }

  override def getNextPageResponse(nextPageRequest: NextPageRequest): ResultSetResponse = {
    val records = getRecords(nextPageRequest)
    ResultSetResponseImpl(ResultSetListResponse(nextPageRequest.fsPath, records.records, records.hasNext),
      records.readBytes, records.readTime)
  }

  override def getFirstPageResponse(firstPageRequest: FirstPageRequest): ResultSetResponse = {
    val resultSetReader = getResultSetReader(firstPageRequest.fsPath)
    val columns = ResultSetServiceImpl.tableMetadataToArray(resultSetReader.getMetaData)
    val records = getRecords(firstPageRequest)
    ResultSetResponseImpl(FirstPageResultSetListResponse(firstPageRequest.fsPath, columns, records.records, records.hasNext),
      records.readBytes, records.readTime)
  }

  override def getFsPathListAndFirstPageResponse(fsPathListAndFirstPageRequest: FsPathListAndFirstPageRequest): ResultSetResponse = {
    val fsPath = new FsPath(fsPathListAndFirstPageRequest.parentFsPath)
    val resultSetPaths = resultSetOperationManager.getResultSetList(fsPath)
    if(resultSetPaths == null || resultSetPaths.isEmpty)
      ResultSetResponseImpl(FsPathListAndFirstPageResultSetListResponse(fsPathListAndFirstPageRequest.fsPath, Array.empty, Array.empty, Array.empty, false),
        0, 0)
    else {
      val firstPageResultSetListResponse = getFirstPageResponse(FirstPageRequest(resultSetPaths(0).getSchemaPath, fsPathListAndFirstPageRequest.pageSize))
      ResultSetResponseImpl(
        FsPathListAndFirstPageResultSetListResponse(fsPathListAndFirstPageRequest.fsPath, firstPageResultSetListResponse.response.asInstanceOf[FirstPageResultSetListResponse].metadata,
        firstPageResultSetListResponse.response.records, resultSetPaths.map(_.getSchemaPath), firstPageResultSetListResponse.response.hasNext),
        firstPageResultSetListResponse.readBytes, firstPageResultSetListResponse.readTime)
    }
  }

  case class ResultSetRecords(records: Array[Array[Any]], hasNext: Boolean, readBytes: Int, readTime: Long)
}

object ResultSetServiceImpl {

  def tableMetadataToArray(metadata: MetaData): Array[util.Map[String, String]] = metadata match {
    case metaData: TableMetaData =>
      metaData.columns.map { column =>
        val columnMap = new util.HashMap[String, String]
        columnMap.put("columnName", column.columnName)
        columnMap.put("dataType", column.dataType.toString())
        columnMap.put("comment", column.comment)
        columnMap
      }
    case _ => throw new ResultSetCacheErrorException(60015, s"Bad usage, cannot cast ${metadata.getClass.getSimpleName} to TableMetaData, please ask admin for help!")
  }

}