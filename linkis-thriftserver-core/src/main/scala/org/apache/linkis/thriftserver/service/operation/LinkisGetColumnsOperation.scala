package org.apache.linkis.thriftserver.service.operation

import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hive.service.cli.operation.GetColumnsOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.service.cli.{OperationState, RowSet, TypeDescriptor}
import org.apache.linkis.thriftserver.service.client.LinkisClient
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-19
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisGetColumnsOperation(parentSession: HiveSession,
                                catalogName: String,
                                schemaName: String,
                                tableName: String,
                                columnName: String) extends GetColumnsOperation(parentSession, catalogName, schemaName, tableName, columnName)
  with LinkisOperation {

  private val rowSet: RowSet = ReflectiveUtils.readSuperField(this, "rowSet")

  override def runInternal(): Unit = runWithState {
    var position = 1
    val columns = LinkisClient.getLinkisClient.getMetaDataClient(parentSession.getUserName).getColumns(schemaName, tableName)
    columns.asScala.foreach {
      columnMap =>
        val columnType = columnMap.get("columnType").toString
        val hiveColumnType = Type.getType(columnType)
        val typeDescriptor = new TypeDescriptor(columnType)
        val rowData = Array[Object](null, schemaName, tableName, columnMap.get("columnName"), Integer.valueOf(hiveColumnType.toJavaSQLType),
          typeDescriptor.getTypeName, typeDescriptor.getColumnSize, null, typeDescriptor.getDecimalDigits,
          hiveColumnType.getNumPrecRadix, Integer.valueOf(1), columnMap.get("columnComment"), null, null, null, null,
          Integer.valueOf(position), "YES", null, null, null, null, "NO")
        this.rowSet.addRow(rowData)
        position += 1
    }
    setHasResultSet(!columns.isEmpty)
  }

  override protected def setOperationState(newState: OperationState): OperationState = setState(newState)

  override protected def registerLoggingContext(): Unit = {}
}