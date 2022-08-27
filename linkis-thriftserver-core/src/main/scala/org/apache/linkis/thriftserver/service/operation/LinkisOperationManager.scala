package org.apache.linkis.thriftserver.service.operation

import java.lang.reflect.Method
import java.util

import org.apache.hive.service.cli.operation._
import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.common.utils.Logging
import org.apache.linkis.thriftserver.service.session.LinkisSessionImpl

/**
 *
 * @date 2022-08-10
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisOperationManager extends OperationManager with Logging {

  private val addOperationMethod: Method = classOf[OperationManager].getDeclaredMethod("addOperation", classOf[Operation])
  addOperationMethod.setAccessible(true)

  protected def newAndAddOperation[T <: Operation](op: => T): T = {
    val operation = op
    addOperationMethod.invoke(this, operation)
    operation
  }

  override def newExecuteStatementOperation(parentSession: HiveSession,
                                            statement: String,
                                            confOverlay: util.Map[String, String],
                                            runAsync: Boolean,
                                            queryTimeout: Long): ExecuteStatementOperation = newAndAddOperation {
    statement.trim.toLowerCase match {
      case "show schemas" | "show databases" =>
        new LinkisCatlogExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, CatalogType.SCHEMAS, null)
      case LinkisOperationManager.SHOW_TABLES(db) =>
        new LinkisCatlogExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, CatalogType.TABLES, db)
      case LinkisOperationManager.SHOW_SCHEMAS(db) =>
        new LinkisCatlogExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, CatalogType.SCHEMAS, db)
      case "show tables" =>
        parentSession match {
          case session: LinkisSessionImpl =>
            if(session.getCurrentDB != null)
              new LinkisCatlogExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, CatalogType.TABLES, session.getCurrentDB)
            else {
              warn(s"Use ${session.getUserName}, Session ${session.getSessionHandle} have not used db, now submit statement 'show tables' to Linkis.")
              new LinkisSQLExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, queryTimeout)
            }
        }
      case LinkisOperationManager.USE_DB(db) =>
        parentSession match {
          case session: LinkisSessionImpl => session.setCurrentDB(db)
        }
        new LinkisSQLExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, queryTimeout)
      case _ =>
        new LinkisSQLExecuteStatementOperation(parentSession, statement, confOverlay, runAsync, queryTimeout)
    }
  }

  override def newGetSchemasOperation(parentSession: HiveSession,
                                      catalogName: String,
                                      schemaName: String): GetSchemasOperation = newAndAddOperation {
    new LinkisGetSchemasOperation(parentSession, catalogName, schemaName)
  }

  override def newGetTablesOperation(parentSession: HiveSession,
                                     catalogName: String,
                                     schemaName: String,
                                     tableName: String,
                                     tableTypes: util.List[String]): MetadataOperation = newAndAddOperation {
    new LinkisGetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes)
  }

  override def newGetColumnsOperation(parentSession: HiveSession,
                                      catalogName: String,
                                      schemaName: String,
                                      tableName: String,
                                      columnName: String): LinkisGetColumnsOperation = newAndAddOperation {
    new LinkisGetColumnsOperation(parentSession, catalogName, schemaName, tableName, columnName)
  }

  override def newGetFunctionsOperation(parentSession: HiveSession,
                                        catalogName: String,
                                        schemaName: String,
                                        functionName: String): GetFunctionsOperation = newAndAddOperation {
    new GetFunctionsOperation(parentSession, catalogName, schemaName, functionName) {
      override def runInternal(): Unit = {
        info(s"User ${parentSession.getUserName} try to access GetFunctionsOperation with catalogName $catalogName, schemaName $schemaName, functionName $functionName, but ignored.")
      }
    }
  }

  override def newGetPrimaryKeysOperation(parentSession: HiveSession,
                                          catalogName: String,
                                          schemaName: String,
                                          tableName: String): GetPrimaryKeysOperation = newAndAddOperation {
    new GetPrimaryKeysOperation(parentSession, catalogName, schemaName, tableName) {
      override def runInternal(): Unit = {
        info(s"User ${parentSession.getUserName} try to access GetPrimaryKeysOperation with catalogName $catalogName, schemaName $schemaName, tableName $tableName, but ignored.")
      }
    }
  }

  override def newGetCrossReferenceOperation(session: HiveSession,
                                             primaryCatalog: String,
                                             primarySchema: String,
                                             primaryTable: String,
                                             foreignCatalog: String,
                                             foreignSchema: String,
                                             foreignTable: String): GetCrossReferenceOperation = newAndAddOperation {
    new GetCrossReferenceOperation(session, primaryCatalog, primarySchema, primaryTable, foreignCatalog, foreignSchema, foreignTable) {
      override def runInternal(): Unit = {
        info(s"User ${parentSession.getUserName} try to access GetCrossReferenceOperation with primaryCatalog $primaryCatalog, primarySchema $primarySchema, primaryTable $primaryTable, foreignCatalog $foreignCatalog, foreignSchema $foreignSchema, foreignTable $foreignTable, but ignored.")
      }
    }
  }

}
object LinkisOperationManager {
  private val SHOW_TABLES = "show tables in `?([^`]+)`?".r.unanchored
  private val SHOW_SCHEMAS = "show schemas like `?([^`]+)`?".r.unanchored
  private val USE_DB = "use `?([^`]+)`?".r.unanchored
}