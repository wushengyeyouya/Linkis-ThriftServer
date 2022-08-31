package org.apache.linkis.thriftserver.service.operation.handler
import java.util

import org.apache.hive.service.cli.session.HiveSession
import org.apache.linkis.common.conf.CommonVars
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration

/**
 * 建议后面优化为 metabase ip list 从数据库获取，以避免 Metabase 扩容时，需重启所有的 Linkis ThriftServer。
 * @date 2022-08-30
 * @author enjoyyin
 * @since 0.5.0
 */
class MetabaseStatementHandler extends StatementHandler {

  private val metabaseServerIpList = CommonVars("linkis.thriftserver.metabase.server.ip.list", "").getValue.split(",")

  override def handle(parentSession: HiveSession, statement: Statement, confOverlay: util.Map[String, String]): Statement = {
    if(metabaseServerIpList.contains(parentSession.getIpAddress)) {
      statement.getProperties.put(LinkisThriftServerConfiguration.JOB_CREATOR.key, "Metabase")
      Statement(statement)
    } else statement
  }

  override val order: Int = 101

}
