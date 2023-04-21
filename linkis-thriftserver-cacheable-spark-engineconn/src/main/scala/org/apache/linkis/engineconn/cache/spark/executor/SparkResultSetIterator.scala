package org.apache.linkis.engineconn.cache.spark.executor

import org.apache.linkis.common.io.MetaData
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.engineconn.cache.storage.ResultSetIterator
import org.apache.linkis.engineconn.computation.executor.execute.EngineExecutionContext
import org.apache.linkis.engineplugin.spark.exception.{NoSupportEngineException, SparkEngineException}
import org.apache.linkis.engineplugin.spark.executor.SQLSession
import org.apache.linkis.engineplugin.spark.utils.EngineUtils
import org.apache.linkis.storage.domain.Column
import org.apache.linkis.storage.resultset.table.{TableMetaData, TableRecord}
import org.apache.spark.sql.Row

class SparkResultSetIterator(columns: Array[Column],
                             maxResult: Int,
                             startTime: Long,
                             jobGroup: String,
                             iterator: java.util.Iterator[Row],
                             engineExecutionContext: EngineExecutionContext) extends ResultSetIterator[TableRecord] with Logging {

  private val tableMetadata = new TableMetaData(columns)
  private var index = 0

  override def getMetaData: MetaData = tableMetadata

  override def reset(): Unit = throw NoSupportEngineException(40011, "Spark do not support to reset resultSet(Spark不支持重置结果集)!")

  override def hasNext: Boolean = {
    val hasNextOrNot = index < maxResult && iterator.hasNext
    if(!hasNextOrNot) {
      logger.warn(s"Time taken: ${System.currentTimeMillis() - startTime}, Fetched $index row(s).")
      engineExecutionContext.appendStdout(
        s"${EngineUtils.getName} >> Time taken: ${System.currentTimeMillis() - startTime}, Fetched $index row(s)."
      )
    }
    hasNextOrNot
  }

  override def next(): TableRecord = {
    Utils.tryThrow({
      val row = iterator.next()
      val r: Array[Any] = columns.indices.map { i => toHiveString(row(i)) }.toArray
      index += 1
      new TableRecord(r)
    }) { t =>
      throw new SparkEngineException(
        READ_RECORD_EXCEPTION.getErrorCode,
        READ_RECORD_EXCEPTION.getErrorDesc,
        t
      )
    }
  }

  override def close(): Unit = {}

  override def getName: String = jobGroup

  private def toHiveString(value: Any): String = {
    value match {
      case value: String => value.replaceAll("\n|\t", " ")
      case value: Double => SQLSession.nf.format(value)
      case value: java.math.BigDecimal => formatDecimal(value)
      case value: Any => value.toString
      case _ => null
    }
  }

  private def formatDecimal(d: java.math.BigDecimal): String = {
    if (null == d || d.compareTo(java.math.BigDecimal.ZERO) == 0) {
      java.math.BigDecimal.ZERO.toPlainString
    } else {
      d.stripTrailingZeros().toPlainString
    }
  }

}
