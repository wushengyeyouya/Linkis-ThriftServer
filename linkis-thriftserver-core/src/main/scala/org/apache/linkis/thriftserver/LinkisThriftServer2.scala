package org.apache.linkis.thriftserver

import java.util.Random

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory
import org.apache.hadoop.hive.common.{JvmPauseMonitor, ServerUtils}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.common.util.HiveStringUtils
import org.apache.hive.http.HttpServer.Builder
import org.apache.hive.http.LlapServlet
import org.apache.hive.service.cli.thrift.{LinkisThriftBinaryCLIService, ThriftHttpCLIService}
import org.apache.hive.service.server.HiveServer2
import org.apache.hive.service.servlet.QueryProfileServlet
import org.apache.linkis.common.conf.Configuration
import org.apache.linkis.common.utils.{Logging, Utils}
import org.apache.linkis.thriftserver.conf.LinkisThriftServerConfiguration
import org.apache.linkis.thriftserver.service.{LinkisCLIService, LinkisCompositeService}
import org.apache.linkis.thriftserver.utils.ReflectiveUtils

/**
 *
 * @date 2022-08-10
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisThriftServer2 extends HiveServer2 with LinkisCompositeService with Logging {

  private val binaryPort = LinkisThriftServerConfiguration.THRIFT_PORT.getValue
  private var cliService: LinkisCLIService = _

  override def init(hiveConf: HiveConf): Unit = synchronized {
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_INPLACE_PROGRESS, false)
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, binaryPort)
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, false)
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_TEZ_INITIALIZE_DEFAULT_SESSIONS, false)
    hiveConf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "mr")

    if(LinkisThriftServerConfiguration.METRICS_ENABLED.getValue) {
      Utils.tryAndWarnMsg(MetricsFactory.init(hiveConf))("Could not initiate the LinkisThriftServer Metrics system.  Metrics may not be reported.")
    }
    cliService = new LinkisCLIService(this)
    addService(cliService)
    val oomHook = new Runnable() {
      override def run(): Unit = {
        LinkisThriftServer2.this.stop()
      }
    }
    val thriftCLIService = if (HiveServer2.isHTTPTransportMode(hiveConf)) new ThriftHttpCLIService(this.cliService, oomHook)
    else LinkisThriftBinaryCLIService(this.cliService, oomHook)
    addService(thriftCLIService)
    ReflectiveUtils.writeSuperField(this, "thriftCLIService", thriftCLIService)
    initService(hiveConf)
    val serverHost = ReflectiveUtils.invokeGetter[String](this, "getServerHost")
    hiveConf.set(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST.varname, serverHost)

    val webUIPort = LinkisThriftServerConfiguration.WEBUI_PORT.getValue
    val uiDisabledInTest = Configuration.IS_TEST_MODE.getValue && webUIPort == LinkisThriftServerConfiguration.WEBUI_PORT.defaultValue
    if (uiDisabledInTest) {
      info("Web UI is disabled in test mode since webui port was not specified.")
    } else if (webUIPort <= 0) {
      info(s"Web UI is disabled since port is set to $webUIPort.")
    } else {
      info(s"Starting Web UI on port $webUIPort.")
      val builder = new Builder("linkisThriftServer")
      builder.setPort(webUIPort).setConf(hiveConf)
      builder.setHost(hiveConf.getVar(ConfVars.HIVE_SERVER2_WEBUI_BIND_HOST))
      builder.setMaxThreads(hiveConf.getIntVar(ConfVars.HIVE_SERVER2_WEBUI_MAX_THREADS))
      builder.setAdmins(hiveConf.getVar(ConfVars.USERS_IN_ADMIN_ROLE))
      builder.setContextAttribute("hive.sm", cliService.getSessionManager)
      hiveConf.set("startcode", String.valueOf(System.currentTimeMillis()))
      if (LinkisThriftServerConfiguration.BDP_SERVER_SECURITY_SSL.acquireNew) {
        val spnegoPrincipal = LinkisThriftServerConfiguration.BDP_SERVER_SECURITY_SSL_KEYSTORE_PATH.acquireNew
        if (StringUtils.isBlank(spnegoPrincipal)) {
          throw new IllegalArgumentException(LinkisThriftServerConfiguration.BDP_SERVER_SECURITY_SSL_KEYSTORE_PATH.key + " Not configured for SSL connection.")
        }

        builder.setKeyStorePassword(LinkisThriftServerConfiguration.BDP_SERVER_SECURITY_SSL_KEYSTORE_PASSWORD.acquireNew)
        builder.setKeyStorePath(spnegoPrincipal)
        builder.setUseSSL(true)
      }
      builder.addServlet("llap", classOf[LlapServlet])
      builder.setContextRootRewriteTarget("/hiveserver2.jsp")
      val webServer = builder.build()
      webServer.addServlet("query_page", "/query_page", classOf[QueryProfileServlet])
      ReflectiveUtils.writeSuperField(this, "webServer", webServer)
    }
    info(s"You can access Linkis-ThriftServer with JDBC URL jdbc:hive2://$serverHost:$binaryPort/")
  }

}

object LinkisThriftServer2 extends Logging {

  private var server: LinkisThriftServer2 = _

  def start(): Unit = if(server == null) synchronized {
    if(server != null) {
      warn("Warning: LinkisThriftServer2 has already been started, do not try to start it again.")
      return
    }
    info("Starting LinkisThriftServer2...")
    val hiveConf = new HiveConf
    ServerUtils.cleanUpScratchDir(hiveConf)
    HiveServer2.scheduleClearDanglingScratchDir(hiveConf, (new Random).nextInt(600))
    Utils.tryCatch {
      server = new LinkisThriftServer2
      server.init(hiveConf)
      server.start()
    } { e =>
      error("Start LinkisThriftServer2 failed!", e)
      if(server != null) server.stop()
      return
    }
    Utils.tryAndWarnMsg {
      val pauseMonitor = new JvmPauseMonitor(hiveConf)
      pauseMonitor.start()
    } ("Could not initiate the JvmPauseMonitor thread. GCs and Pauses may not be warned upon.")
    HiveStringUtils.startupShutdownMessage(classOf[LinkisThriftServer2], Array.empty, logger)
    info("LinkisThriftServer2 started!")
  }

}