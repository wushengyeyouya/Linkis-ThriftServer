package org.apache.linkis.engineconn.cache.storage

import java.io.Closeable

import org.apache.linkis.common.io.{MetaData, Record}

/**
  * Created by enjoyyin.
  */
abstract class ResultSetIterator[V <: Record] extends Iterator[V] with Closeable {

  def getMetaData: MetaData

  def reset(): Unit

  def getName: String

}