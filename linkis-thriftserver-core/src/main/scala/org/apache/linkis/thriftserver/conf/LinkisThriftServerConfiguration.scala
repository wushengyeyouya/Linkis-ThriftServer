package org.apache.linkis.thriftserver.conf

import java.io.File

import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes
import org.apache.linkis.common.conf.{CommonVars, TimeType}
import org.apache.linkis.common.utils.Utils

/**
 *
 * @date 2022-08-11
 * @author enjoyyin
 * @since 0.5.0
 */
object LinkisThriftServerConfiguration {

  val AUTHENTICATION = CommonVars("linkis.thriftserver.authentication", AuthTypes.NONE.getAuthName, "only NONE, SASL, LDAP, CUSTOM are supported.")
  val ENGINE_TYPE = CommonVars("linkis.thriftserver.job.engine.type", "spark")
  val ENGINE_VERSION = CommonVars("linkis.thriftserver.job.engine.version", "2.4.3")
  val JOB_CREATOR = CommonVars("job.creator", "ThriftServer") // 为了方便 JDBC URL设置 creator，这里简化该参数的key。
  val RUN_TYPE_LIST = CommonVars("linkis.thriftserver.runType.list", "spark->sql,hive->hql,presto->psql,trino->tsql")
  val runTypeMap = RUN_TYPE_LIST.getValue.split(",").map { one =>
    val runTypeArray = one.split("->")
    runTypeArray(0) -> runTypeArray(1)
  }.toMap

  val LINKIS_JOB_CLIENT_CLASS = CommonVars("linkis.thriftserver.job.client.class", "")
  val LINKIS_BINARY_CLI_SERVICE_CLASS = CommonVars("linkis.thriftserver.binary.cli.service.class", "")

  val METRICS_ENABLED = CommonVars("linkis.thriftserver.metrics.enabled", true)

  val THRIFT_PORT = CommonVars("linkis.thriftserver.port", 10000)
  val WEBUI_PORT = CommonVars("linkis.thriftserver.webui.port", 10002)

  val OPERATION_LOG_ENABLED = CommonVars("linkis.thriftserver.operation.log.enabled", true)
  val OPERATION_LOG_LOCATION = CommonVars("linkis.thriftserver.operation.log.location", "logs" + File.separator + Utils.getJvmUser + File.separator + "operation_logs")
  val QUERY_TIMEOUT_SECONDS = CommonVars("linkis.thriftserver.query.timeout.seconds", new TimeType("0s"))

  val SESSION_CHECK_INTERVAL = CommonVars("linkis.thriftserver.session.check.interval", new TimeType("1h"))
  val IDLE_SESSION_TIMEOUT = CommonVars("linkis.thriftserver.idle.session.timeout", new TimeType("1d"))
  val IDLE_SESSION_CHECK_ENABLE = CommonVars("linkis.thriftserver.idle.session.check.enable", true)
  val SESSION_HOOK = CommonVars("linkis.thriftserver.session.hook", "")

  val RESULTSET_DEFAULT_FETCH_SIZE = CommonVars("linkis.thriftserver.resultset.default.fetch.size", 1000)

  val ASYNC_EXEC_THREAD_SIZE = CommonVars("linkis.thriftserver.async.exec.threads", 200)
  val ASYNC_EXEC_SHUTDOWN_TIMEOUT = CommonVars("linkis.thriftserver.async.exec.shutdown.timeout", new TimeType("10s"))

  /**
   * copied from Linkis ServerConfiguration
   */
  val BDP_SERVER_SECURITY_SSL = CommonVars("wds.linkis.server.security.ssl", false)
  val BDP_SERVER_SECURITY_SSL_EXCLUDE_PROTOCOLS = CommonVars("wds.linkis.server.security.ssl.excludeProtocols", "SSLv2,SSLv3")
  val BDP_SERVER_SECURITY_SSL_KEYSTORE_PATH = CommonVars("wds.linkis.server.security.ssl.keystore.path", "")
  val BDP_SERVER_SECURITY_SSL_KEYSTORE_TYPE = CommonVars("wds.linkis.server.security.ssl.keystore.type", "JKS")
  val BDP_SERVER_SECURITY_SSL_KEYSTORE_PASSWORD = CommonVars("wds.linkis.server.security.ssl.keystore.password", "")
}
