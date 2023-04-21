package org.apache.linkis.orchestrator

import org.apache.linkis.orchestrator.core.AbstractOrchestration
import org.apache.linkis.orchestrator.plans.physical.ExecTask

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
object OrchestrationUtils {

  def getPhysicalPlan(orchestration: Orchestration): ExecTask = orchestration match {
    case abstractOrchestration: AbstractOrchestration => abstractOrchestration.physicalPlan
  }

}
