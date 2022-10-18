package org.apache.linkis.thriftserver.service.operation.handler
import java.util

import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.common.conf.CommonVars
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration

/**
 * 建议后面优化为 tableau ip list 从数据库获取，以避免 Tableau 扩容时，需重启所有的 Linkis ThriftServer。
 * @date 2022-08-30
 * @author enjoyyin
 * @since 0.5.0
 */
class TableauStatementHandler extends StatementHandler {

  private val tableauClientIpList = CommonVars("linkis.thriftserver.tableau.client.ip.list", "").getValue.split(",")
  private val tableauServerIpList = CommonVars("linkis.thriftserver.tableau.server.ip.list", "").getValue.split(",")

  override def handle(parentSession: HiveSession, statement: Statement, confOverlay: util.Map[String, String]): Statement = {
    if(tableauClientIpList.contains(parentSession.getIpAddress)) {
      statement.getProperties.put(LinkisThriftServerConfiguration.JOB_CREATOR.key, "TableauClient")
      Statement(s"-- Tableau Client(${parentSession.getIpAddress})\n" + statement.getSQL, statement)
    } else if(tableauServerIpList.contains(parentSession.getIpAddress)) {
      statement.getProperties.put(LinkisThriftServerConfiguration.JOB_CREATOR.key, "TableauServer")
      Statement(s"-- Tableau Server(${parentSession.getIpAddress})\n" + statement.getSQL, statement)
    } else statement
  }

  override val order: Int = 100

}
