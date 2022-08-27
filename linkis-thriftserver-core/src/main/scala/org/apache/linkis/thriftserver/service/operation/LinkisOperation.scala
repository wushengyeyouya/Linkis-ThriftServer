package org.apache.linkis.thriftserver.service.operation

import org.apache.hive.service.cli.{HiveSQLException, OperationState}
import org.apache.linkis.common.utils.{Logging, Utils}

/**
 *
 * @date 2022-08-11
 * @author enjoyyin
 * @since 0.5.0
 */
trait LinkisOperation extends Logging {

  protected def setOperationState(newState: OperationState): OperationState

  protected def runWithState[R](op: => R): R = {
    setOperationState(OperationState.RUNNING)
    val value = Utils.tryThrow {
      op
    } { e =>
      setOperationState(OperationState.ERROR)
      new HiveSQLException(e)
    }
    setOperationState(OperationState.FINISHED)
    value
  }

}
