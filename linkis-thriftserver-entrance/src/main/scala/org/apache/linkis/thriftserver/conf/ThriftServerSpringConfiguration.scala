package org.apache.linkis.thriftserver.conf

import javax.annotation.PostConstruct
import org.apache.linkis.common.utils.Logging
import org.apache.linkis.entrance.orchestrator.EntranceOrchestrationFactory
import org.apache.linkis.orchestrator.computation.ComputationOrchestratorSessionFactory
import org.apache.linkis.thriftserver.orchestrator.operation.ResultSetCacheOperation
import org.springframework.context.annotation.Configuration

/**
 *
 * @date 2022-09-30
 * @author enjoyyin
 * @since 0.5.0
 */
@Configuration
class ThriftServerSpringConfiguration extends Logging {

  @PostConstruct
  def init(): Unit = {
    val sessionBuilder = ComputationOrchestratorSessionFactory.getOrCreateExecutionFactory()
      .createSessionBuilder(EntranceOrchestrationFactory.ENTRANCE_ORCHESTRATOR_DEFAULT_ID)
    sessionBuilder.withOperationExtensions(extensions => extensions.injectOperation(_ => new ResultSetCacheOperation))
    info("Registered ResultSetCacheOperation.")
  }

}
