package org.apache.linkis.thriftserver.service.operation

import org.apache.hive.service.cli.{OperationState, RowSet}
import org.apache.hive.service.cli.operation.GetSchemasOperation
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
class LinkisGetSchemasOperation(parentSession: HiveSession,
                                catalogName: String,
                                schemaName: String) extends GetSchemasOperation(parentSession, catalogName, schemaName) with LinkisOperation {

  private val rowSet: RowSet = ReflectiveUtils.readSuperField(this, "rowSet")

  override def runInternal(): Unit = runWithState {
    val dbs = LinkisClient.getLinkisClient.getMetaDataClient(parentSession.getUserName).getDBS
    dbs.asScala.foreach { dbName =>
      rowSet.addRow(Array[AnyRef](dbName, ""))
    }
    setHasResultSet(!dbs.isEmpty)
  }

  override protected def setOperationState(newState: OperationState): OperationState = setState(newState)

  override protected def registerLoggingContext(): Unit = {}

}
