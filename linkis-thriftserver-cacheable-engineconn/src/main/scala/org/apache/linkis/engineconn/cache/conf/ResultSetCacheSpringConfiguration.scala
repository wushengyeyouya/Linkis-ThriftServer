package org.apache.linkis.engineconn.cache.conf

import java.util.concurrent.ScheduledThreadPoolExecutor

import org.apache.linkis.common.utils.Utils
import org.apache.linkis.engineconn.cache.conf.ResultSetCacheConfiguration._
import org.apache.linkis.engineconn.cache.operation.{ResultSetOperationManager, ResultSetOperationManagerImpl}
import org.apache.linkis.engineconn.cache.receiver.ResultSetCacheReceiverChooser
import org.apache.linkis.engineconn.cache.service.{ResultSetService, ResultSetServiceImpl}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.{Bean, Configuration}


@Configuration
class ResultSetCacheSpringConfiguration {

  private val executorService = new ScheduledThreadPoolExecutor(ENGINE_RESULT_SET_CACHE_READ_THREAD.getValue,
    Utils.threadFactory("ResultSet-Cache-Thread-", true))

  @Bean
  @ConditionalOnMissingBean
  def createResultSetOperationManager(): ResultSetOperationManager = {
    val resultSetOperationManager = new ResultSetOperationManagerImpl
    resultSetOperationManager.setExecutorService(executorService)
    resultSetOperationManager.init()
    resultSetOperationManager
  }

  @Bean
  @ConditionalOnMissingBean
  @Autowired
  def createResultSetService(resultSetOperationManager: ResultSetOperationManager): ResultSetService = {
    new ResultSetServiceImpl(resultSetOperationManager)
  }

  @Bean
  @ConditionalOnMissingBean
  @Autowired
  def createResultSetCacheReceiverChooser(resultSetService: ResultSetService): ResultSetCacheReceiverChooser = {
    new ResultSetCacheReceiverChooser(resultSetService)
  }

//  @Bean
//  @Autowired
//  def createResultSetCacheRestfulApi(resultSetService: ResultSetService): ResultSetCacheRestfulApi = {
//    val resultSetCacheRestfulApi = new ResultSetCacheRestfulApi
//    resultSetCacheRestfulApi.setResultSetService(resultSetService)
//    resultSetCacheRestfulApi
//  }

}