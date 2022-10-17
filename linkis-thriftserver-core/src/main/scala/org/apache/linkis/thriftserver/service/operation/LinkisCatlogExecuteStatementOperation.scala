package org.apache.linkis.thriftserver.service.operation

import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.serde2.thrift.Type
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.thriftserver.service.client.{LinkisClient, MetadataClient}
import org.apache.linkis.thriftserver.service.operation.CatalogType.CatalogType
import org.apache.linkis.thriftserver.service.operation.handler.{ProxyUserUtils, Statement}

import scala.collection.JavaConverters._

/**
 *
 * @date 2022-08-26
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisCatlogExecuteStatementOperation(parentSession: HiveSession,
                                            statement: Statement,
                                            confOverlay: util.Map[String, String],
                                            runInBackground: Boolean,
                                            catalogType: CatalogType,
                                            filter: String) extends ExecuteStatementOperation(parentSession, statement.getSQL, confOverlay, runInBackground) with LinkisOperation {

  private val schema: TableSchema = catalogType match {
    case CatalogType.SCHEMAS =>
      new TableSchema().addStringColumn("database_name", "Schema Name.")
    case CatalogType.TABLES =>
      new TableSchema().addStringColumn("database", "Database Name.")
        .addStringColumn("tableName", "Table Name.")
        .addPrimitiveColumn("isTemporary", Type.BOOLEAN_TYPE, "Is temporary table?")
  }
  private val rowSet: RowSet = RowSetFactory.create(schema, getProtocolVersion, false)

  override def runInternal(): Unit = runWithState {
    val executeUser = ProxyUserUtils.getExecuteUser(parentSession, statement)
    if(executeUser != parentSession.getUserName) {
      info(s"$getHandle changed executeUser from session.user: ${parentSession.getUserName} to proxy.user: $executeUser.")
    }
    val metaDataClient = LinkisClient.getLinkisClient.getMetaDataClient(executeUser)
    catalogType match {
      case CatalogType.SCHEMAS =>
        val filterOp: String => Boolean = str => if(StringUtils.isBlank(filter)) true else str == filter
        val dbs = metaDataClient.getDBS
        setHasResultSet(!dbs.isEmpty)
        dbs.asScala.filter(filterOp).foreach { str =>
          rowSet.addRow(Array[Object](str))
        }
      case CatalogType.TABLES =>
        filter match {
          case "*" | "ALL" | "all" =>
            metaDataClient.getDBS.asScala.foreach(fetchTables(metaDataClient, _))
          case _ =>
            fetchTables(metaDataClient, filter)
        }
        setHasResultSet(rowSet.numRows() > 0)
    }
  }

  private def fetchTables(metaDataClient: MetadataClient, db: String): Unit = {
    val tables = metaDataClient.getTables(db)
    tables.asScala.foreach { str =>
      rowSet.addRow(Array[Object](db, str.get("tableName"), java.lang.Boolean.FALSE))
    }
  }

  override protected def setOperationState(newState: OperationState): OperationState = setState(newState)

  override def cancel(operationState: OperationState): Unit = {
    setState(operationState)
    cleanupOperationLog()
  }

  override def close(): Unit = {
    setState(OperationState.CLOSED)
    cleanupOperationLog()
  }

  override def getResultSetSchema: TableSchema = schema

  override def getNextRowSet(fetchOrientation: FetchOrientation, maxRows: Long): RowSet = {
    this.assertState(util.Arrays.asList(OperationState.FINISHED))
    this.validateDefaultFetchOrientation(fetchOrientation)
    if (fetchOrientation == FetchOrientation.FETCH_FIRST) {
      rowSet.setStartOffset(0L)
    }
    rowSet.extractSubset(maxRows.toInt)
  }

  override protected def registerLoggingContext(): Unit = {}

}

object CatalogType extends Enumeration {
  type CatalogType = Value
  val SCHEMAS, TABLES = Value
}