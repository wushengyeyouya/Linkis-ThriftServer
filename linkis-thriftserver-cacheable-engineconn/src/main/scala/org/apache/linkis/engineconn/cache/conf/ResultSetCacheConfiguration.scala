package org.apache.linkis.engineconn.cache.conf

import org.apache.linkis.common.conf.{ByteType, CommonVars, TimeType}

/**
 *
 * @author enjoyyin
 * @since 0.5.0
 */
object ResultSetCacheConfiguration {

  val ENGINE_RESULT_SET_CACHE_READ_THREAD: CommonVars[Int] = CommonVars("wds.linkis.engine.resultset.cache.read.thread", 10)

  val ENGINE_RESULT_SET_CACHE_READ_WAIT_TIME: CommonVars[TimeType] = CommonVars("wds.linkis.engine.resultset.cache.read.wait", new TimeType("20ms"))

  val ENGINE_RESULT_SET_CACHE_EXISTS_TIME: CommonVars[TimeType] = CommonVars("wds.linkis.engine.resultset.cache.exists.time", new TimeType("2m"))

  val ENGINE_RESULT_SET_CACHE_MEMORY_SIZE: CommonVars[Double] = CommonVars("wds.linkis.engine.resultset.cache.memory", 0.5)

  val ENGINE_RESULT_SET_CACHE_BLOCK_FREE_TIME: CommonVars[TimeType] = CommonVars("wds.linkis.engine.resultset.cache.block.free.max", new TimeType("1m"))

  val ENGINE_RESULT_SET_CACHE_ENABLE: CommonVars[Boolean] = CommonVars("wds.linkis.engine.resultset.cache.enable", false)

  val ENGINE_RESULT_SET_PAGE_SIZE_MAX: CommonVars[Int] = CommonVars("wds.linkis.engine.resultset.page.size.max", 5000)

  val ENGINE_RESULT_SET_CACHE_COMPRESS_SIZE: CommonVars[ByteType] = CommonVars("wds.linkis.engine.resultset.cache.compress.size", new ByteType("1m"))

}
