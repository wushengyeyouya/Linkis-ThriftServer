package org.apache.linkis.thriftserver.conf

import org.apache.linkis.common.conf.{CommonVars, TimeType}
import org.apache.linkis.manager.label.entity.engine.RunType.SQL

/**
 *
 * @date 2022-12-22
 * @author enjoyyin
 * @since 0.5.0
 */
object ThriftServerConfiguration {

  val THRIFT_SERVER_CODE_TYPE = CommonVars("linkis.thriftserver.code.type", SQL.toString)

  val JOB_MAX_EXECUTION_TIME = CommonVars("linkis.thriftserver.job.execution.time", new TimeType("30m"))

  val THRIFT_SERVER_RESULT_SET_PAGE_SIZE = CommonVars("linkis.thriftserver.result.page.size", 1000)

}
