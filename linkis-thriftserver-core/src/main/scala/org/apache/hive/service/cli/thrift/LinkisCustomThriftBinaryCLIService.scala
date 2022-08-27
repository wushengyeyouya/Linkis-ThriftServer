package org.apache.hive.service.cli.thrift

import java.util.concurrent.{Future, SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import org.apache.hadoop.hive.common.auth.HiveAuthUtils
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes
import org.apache.hive.service.auth.PlainSaslHelper
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.thrift.ThriftCLIService.ThriftCLIServerContext
import org.apache.hive.service.server.ThreadFactoryWithGarbageCleanup
import org.apache.linkis.common.utils.Utils
import org.apache.thrift.protocol.TBinaryProtocol.Factory
import org.apache.thrift.protocol.TProtocol
import org.apache.thrift.server.TThreadPoolServer.Args
import org.apache.thrift.server.{ServerContext, TServerEventHandler, TThreadPoolServer}
import org.apache.thrift.transport._

/**
 *
 * @date 2022-08-20
 * @author enjoyyin
 * @since 0.5.0
 */
class LinkisCustomThriftBinaryCLIService(cliService: CLIService, oomHook: Runnable)
  extends LinkisThriftBinaryCLIService(cliService, oomHook) {

  /**
   * We should use a special login style to ensure the ability of login with pin + token,
   * so we will try to login in `CUSTOM` styles.
   */
  override protected def getAuthentication: String = AuthTypes.CUSTOM.getAuthName

  override def run(): Unit = {
    val executorService = new ThreadPoolExecutor(this.minWorkerThreads, this.maxWorkerThreads, this.workerKeepAliveTime,
      TimeUnit.SECONDS, new SynchronousQueue[Runnable], new ThreadFactoryWithGarbageCleanup("Linkis-ThriftServer-Handler-Pool")) {
      override protected def afterExecute(r: Runnable, t: Throwable): Unit = {
        super.afterExecute(r, t)
        var exception = t
        r match {
          case future: Future[_] if t == null =>
            Utils.tryCatch(if (future.isDone) future.get) {
              case _: InterruptedException => Thread.currentThread.interrupt()
              case e => exception = e
            }
          case _ =>
        }
        t match {
          case _: OutOfMemoryError => oomHook.run()
          case _ =>
        }
      }
    }
    val transportFactory = new LinkisTTransportFactory
    val processorFactory = PlainSaslHelper.getPlainProcessorFactory(this)
    val serverSocket = HiveAuthUtils.getServerSocket(this.hiveHost, this.portNum)
    val maxMessageSize = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE)
    val requestTimeout = hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT, TimeUnit.SECONDS)
    val beBackoffSlotLength = hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH, TimeUnit.MILLISECONDS)
    val args = new Args(serverSocket).processorFactory(processorFactory).transportFactory(transportFactory)
      .protocolFactory(new Factory()).inputProtocolFactory(new Factory(true, true, maxMessageSize, maxMessageSize))
      .requestTimeout(requestTimeout.toInt).requestTimeoutUnit(TimeUnit.SECONDS)
      .beBackoffSlotLength(beBackoffSlotLength.toInt).beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS).executorService(executorService)
    this.server = new TThreadPoolServer(args)
    this.server.setServerEventHandler(new TServerEventHandler() {
      override def createContext(input: TProtocol, output: TProtocol): ServerContext = {
        val metrics = MetricsFactory.getInstance()
        if (metrics != null) {
          Utils.tryAndWarnMsg {
            metrics.incrementCounter("open_connections")
            metrics.incrementCounter("cumulative_connection_count")
          }("Error Reporting JDO operation to Metrics system.")
        }
        new ThriftCLIServerContext
      }

      override def deleteContext(serverContext: ServerContext, input: TProtocol, output: TProtocol): Unit = {
        val metrics = MetricsFactory.getInstance()
        if (metrics != null) {
          Utils.tryAndWarnMsg {
            metrics.decrementCounter("open_connections")
          } ("Error Reporting JDO operation to Metrics system.")
        }
        serverContext match {
          case context: ThriftCLIServerContext =>
            val sessionHandle = context.getSessionHandle
            if (sessionHandle != null) {
              info(s"Session $sessionHandle disconnected without closing properly. ")
              Utils.tryAndWarnMsg {
                val close = cliService.getSessionManager.getSession(sessionHandle).getHiveConf.getBoolVar(ConfVars.HIVE_SERVER2_CLOSE_SESSION_ON_DISCONNECT)
                info(s"${if(close) "" else "Not "} Closing the session: $sessionHandle.")
                if (close) {
                  cliService.closeSession(sessionHandle)
                }
              } ("Failed to close session: " + sessionHandle)
            }
        }
      }

      override def preServe(): Unit = {}

      override def processContext(serverContext: ServerContext, input: TTransport, output: TTransport) {
        currentServerContext.set(serverContext)
      }

    })
    info(s"Starting ${classOf[LinkisCustomThriftBinaryCLIService].getSimpleName} on port $portNum with $minWorkerThreads...$maxWorkerThreads worker threads.")
    server.serve()
  }

}
