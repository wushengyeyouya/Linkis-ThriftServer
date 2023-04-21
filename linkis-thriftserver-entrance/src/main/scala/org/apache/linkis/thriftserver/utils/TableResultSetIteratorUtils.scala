package org.apache.linkis.thriftserver.utils

import java.util

import org.apache.linkis.common.io.{MetaData, Record}
import org.apache.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import org.apache.linkis.thriftserver.exception.EntranceThriftServerErrorException

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
object TableResultSetIteratorUtils {

  def tableMetadataToArray(metadata: MetaData): Array[util.Map[String, String]] = metadata match {
    case metaData: TableMetaData =>
      metaData.columns.map { column =>
        val columnMap = new util.HashMap[String, String]
        columnMap.put("columnName", column.columnName)
        columnMap.put("dataType", column.dataType.toString())
        columnMap.put("comment", column.comment)
        columnMap
      }
    case _ => throw new EntranceThriftServerErrorException(60015, s"Bad usage, cannot cast ${metadata.getClass.getSimpleName} to TableMetaData, please ask admin for help!")
  }

  def tableMetadataToList(metadata: MetaData): util.List[util.Map[String, String]] = {
    val metaDataArray = tableMetadataToArray(metadata)
    util.Arrays.asList(metaDataArray: _*)
  }

  def tableRecordToList(record: Record): util.List[Any] = record match {
    case tableRecord: TableRecord => util.Arrays.asList(tableRecord.row: _*)
    case _ => throw new EntranceThriftServerErrorException(60015, s"Bad usage, cannot cast ${record.getClass.getSimpleName} to TableRecord, please ask admin for help!")
  }

}