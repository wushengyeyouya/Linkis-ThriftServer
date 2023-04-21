package org.apache.linkis.thriftserver.orchestrator.operation

import org.apache.linkis.orchestrator.Orchestration
import org.apache.linkis.orchestrator.extensions.operation.Operation

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
class ResultSetCacheOperation extends Operation[ResultSetCacheEndpoint] {

  override def apply(orchestration: Orchestration): ResultSetCacheEndpoint =
    new ResultSetCacheEndpointImpl(orchestration)

  override def getName: String = ResultSetCacheOperation.OPERATION_NAME

}

object ResultSetCacheOperation {
  val OPERATION_NAME = "resultSetCacheOperation"
}