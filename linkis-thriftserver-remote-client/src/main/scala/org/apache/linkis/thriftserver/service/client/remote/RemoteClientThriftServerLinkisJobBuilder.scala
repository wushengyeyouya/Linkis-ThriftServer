package org.apache.linkis.thriftserver.service.client.remote

import org.apache.linkis.computation.client.LinkisJobClient
import org.apache.linkis.computation.client.utils.LabelKeyUtils
import org.apache.linkis.thriftserver.service.client.{ThriftServerLinkisJob, ThriftServerLinkisJobBuilder}

/**
 *
 * @date 2022-09-30
 * @author enjoyyin
 * @since 0.5.0
 */
class RemoteClientThriftServerLinkisJobBuilder extends ThriftServerLinkisJobBuilder {

  private val builder = LinkisJobClient.interactive.builder()

  private def returnThis(op: => ()): RemoteClientThriftServerLinkisJobBuilder = {
    op
    this
  }

  override def addExecuteUser(executeUser: String): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addExecuteUser(executeUser)
  }

  override def addSubmitUser(submitUser: String): RemoteClientThriftServerLinkisJobBuilder = this

  override def addLabel(key: String, value: Any): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addLabel(key, value)
  }

  override def addEngineType(engineType: String): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addLabel(LabelKeyUtils.ENGINE_TYPE_LABEL_KEY, engineType)
  }

  override def addCreator(creator: String): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.setCreator(creator)
  }

  override def addJobContent(key: String, value: Any): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addJobContent(key, value)
  }

  override def addSource(key: String, value: Any): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addSource(key, value)
  }

  override def addVariable(key: String, value: Any): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addVariable(key, value)
  }

  override def addStartupParam(key: String, value: Any): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addStartupParam(key, value)
  }

  override def addRuntimeParam(key: String, value: Any): RemoteClientThriftServerLinkisJobBuilder = returnThis {
    builder.addRuntimeParam(key, value)
  }

  override def build(): ThriftServerLinkisJob = new RemoteClientThriftServerLinkisJob(builder.build())
}
