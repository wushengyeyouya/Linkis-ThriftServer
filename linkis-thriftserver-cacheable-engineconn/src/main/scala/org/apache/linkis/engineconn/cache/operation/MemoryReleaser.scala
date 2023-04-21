package org.apache.linkis.engineconn.cache.operation

/**
  * Created by enjoyyin.
  */
trait MemoryReleaser {

  def tryRelease(numBytes: Int): Int

}
