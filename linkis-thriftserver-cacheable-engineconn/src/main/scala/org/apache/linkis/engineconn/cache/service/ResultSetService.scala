package org.apache.linkis.engineconn.cache.service

import org.apache.linkis.engineconn.cache.protocol._

/**
  * Created by enjoyyin.
  */
trait ResultSetService {

  def getNextPage(nextPageRequest: NextPageRequest): ResultSetListResponse

  def getFirstPage(firstPageRequest: FirstPageRequest): FirstPageResultSetListResponse

  def getFsPathListAndFirstPage(fsPathListAndFirstPageRequest: FsPathListAndFirstPageRequest): FsPathListAndFirstPageResultSetListResponse

  def getNextPageResponse(nextPageRequest: NextPageRequest): ResultSetResponse

  def getFirstPageResponse(firstPageRequest: FirstPageRequest): ResultSetResponse

  def getFsPathListAndFirstPageResponse(fsPathListAndFirstPageRequest: FsPathListAndFirstPageRequest): ResultSetResponse

}

abstract class AbstractResultSetService extends ResultSetService {

  override def getNextPage(nextPageRequest: NextPageRequest): ResultSetListResponse =
    getNextPageResponse(nextPageRequest).response.asInstanceOf[ResultSetListResponse]

  override def getFirstPage(firstPageRequest: FirstPageRequest): FirstPageResultSetListResponse =
    getFirstPageResponse(firstPageRequest).response.asInstanceOf[FirstPageResultSetListResponse]

  override def getFsPathListAndFirstPage(fsPathListAndFirstPageRequest: FsPathListAndFirstPageRequest): FsPathListAndFirstPageResultSetListResponse =
    getFsPathListAndFirstPageResponse(fsPathListAndFirstPageRequest).response.asInstanceOf[FsPathListAndFirstPageResultSetListResponse]

}
