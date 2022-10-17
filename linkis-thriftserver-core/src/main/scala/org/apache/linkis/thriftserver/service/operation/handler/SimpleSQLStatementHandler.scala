package org.apache.linkis.thriftserver.service.operation.handler
import java.util

import org.apache.hive.service.cli.session.HiveSession

/**
 *
 * @date 2022-10-17
 * @author enjoyyin
 * @since 0.5.0
 */
class SimpleSQLStatementHandler extends StatementHandler {

  override def handle(parentSession: HiveSession, statement: Statement,
                      confOverlay: util.Map[String, String]): Statement = {
    val simpleSQL = statement.getSQL.split("\n").filter { line =>
      val trimLine = line.trim
      trimLine.nonEmpty && !trimLine.startsWith("--")
    }.mkString("\n")
    new StatementImpl(statement.getSQL, simpleSQL, statement.getProperties, statement.getErrorMsg)
  }

  override val order: Int = 50
}
