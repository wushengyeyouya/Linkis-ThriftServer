package org.apache.linkis.thriftserver.service.operation.handler
import java.util

import org.apache.commons.lang3.StringUtils
import org.apache.hive.service.cli.session.HiveSession

/**
 *
 * @date 2022-08-30
 * @author enjoyyin
 * @since 0.5.0
 */
class PropertiesStatementHandler extends StatementHandler {

  override def handle(parentSession: HiveSession, statement: Statement, confOverlay: util.Map[String, String]): Statement = {
    statement.getSQL.split("\n").filter(StringUtils.isNotBlank).foreach { sql =>
      sql.trim match {
        case PropertiesStatementHandler.PROPERTIES_REGEX(key, value) =>
          statement.getProperties.put(key, value)
        case _ =>
      }
    }
    statement
  }

  override val order: Int = 0
}
object PropertiesStatementHandler {
  private val PROPERTIES_REGEX = "--\\s*set\\s+(\\S+)\\s*=\\s*(\\S+)\\s*".r.unanchored
}