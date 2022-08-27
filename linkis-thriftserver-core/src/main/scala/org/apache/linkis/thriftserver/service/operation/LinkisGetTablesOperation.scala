package org.apache.linkis.thriftserver.service.operation

import java.util

import org.apache.hadoop.hive.metastore.TableType
import org.apache.hive.service.cli.{OperationState, RowSet}
import org.apache.hive.service.cli.operation.GetTablesOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.thriftserver.service.client.LinkisClient
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-18
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisGetTablesOperation(parentSession: HiveSession,
                               catalogName: String,
                               schemaName: String,
                               tableName: String,
                               tableTypes: util.List[String])
  extends GetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes) with LinkisOperation {

  private val rowSet: RowSet = ReflectiveUtils.readSuperField(this, "rowSet")

  override def runInternal(): Unit = runWithState {
    // ignore tableTypes
    val tables = LinkisClient.getLinkisClient.getMetaDataClient(parentSession.getUserName).getTables(schemaName)
    tables.asScala.foreach { table =>
      val tableType = if(table.get("isView").asInstanceOf[Boolean]) TableType.VIRTUAL_VIEW else TableType.MANAGED_TABLE
      rowSet.addRow(Array[AnyRef]("", schemaName, table.get("tableName"), tableType, table.get("comment"), null, null, null, null, null));
    }
    setHasResultSet(!tables.isEmpty)
  }

  override protected def setOperationState(newState: OperationState): OperationState = setState(newState)

  override protected def registerLoggingContext(): Unit = {}

}
