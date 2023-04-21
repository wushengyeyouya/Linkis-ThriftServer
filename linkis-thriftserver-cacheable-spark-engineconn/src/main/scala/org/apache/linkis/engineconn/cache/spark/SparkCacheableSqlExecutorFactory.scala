/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.linkis.engineconn.cache.spark

import org.apache.linkis.engineconn.cache.spark.executor.SparkSqlCacheableExecutor
import org.apache.linkis.engineconn.common.creation.EngineCreationContext
import org.apache.linkis.engineconn.common.engineconn.EngineConn
import org.apache.linkis.engineconn.computation.executor.creation.ComputationExecutorFactory
import org.apache.linkis.engineconn.computation.executor.execute.ComputationExecutor
import org.apache.linkis.engineplugin.spark.entity.SparkEngineSession
import org.apache.linkis.engineplugin.spark.exception.NotSupportSparkSqlTypeException
import org.apache.linkis.manager.label.entity.{Feature, Label}
import org.apache.linkis.manager.label.entity.cache.CacheLabel
import org.apache.linkis.manager.label.entity.engine.RunType
import org.apache.linkis.manager.label.entity.engine.RunType.RunType

/**
  *
  */
class SparkCacheableSqlExecutorFactory extends ComputationExecutorFactory {

  override protected def newExecutor(id: Int,
                                     engineCreationContext: EngineCreationContext,
                                     engineConn: EngineConn,
                                     label: Array[Label[_]]): ComputationExecutor = {
    val isCacheableOpened = label.exists {
      case cacheLabel: CacheLabel => cacheLabel.getFeature == Feature.SUITABLE
      case _ => false
    }
    if(isCacheableOpened) {
      throw NotSupportSparkSqlTypeException("CacheLabel is invalid, failed to create sparkCacheableSql executor.")
    }
    engineConn.getEngineConnSession match {
      case sparkEngineSession: SparkEngineSession =>
        new SparkSqlCacheableExecutor(sparkEngineSession, id)
      case _ =>
        throw NotSupportSparkSqlTypeException("Invalid EngineConn engine session obj, failed to create sparkSql executor")
    }
  }

  override protected def getRunType: RunType = RunType.JDBC
}
