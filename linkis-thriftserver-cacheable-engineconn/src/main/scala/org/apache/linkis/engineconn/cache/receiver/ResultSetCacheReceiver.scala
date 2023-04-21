package org.apache.linkis.engineconn.cache.receiver

import org.apache.linkis.common.utils.Logging
import org.apache.linkis.engineconn.cache.protocol.{FirstPageRequest, FsPathListAndFirstPageRequest, NextPageRequest, ResultSetRequestProtocol}
import org.apache.linkis.engineconn.cache.service.ResultSetService
import org.apache.linkis.rpc.{RPCMessageEvent, Receiver, ReceiverChooser, Sender}

import scala.concurrent.duration.Duration

/**
  * Created by enjoyyin.
  */
class ResultSetCacheReceiver(resultSetService: ResultSetService) extends Receiver with Logging {

  override def receive(message: Any, sender: Sender): Unit = {}

  override def receiveAndReply(message: Any, sender: Sender): Any = message match {
    case nextPageRequest: NextPageRequest =>
      logger.info(s"client $sender fetch nextPage with params: $nextPageRequest.")
      resultSetService.getNextPage(nextPageRequest)
    case firstPageRequest: FirstPageRequest =>
      logger.info(s"client $sender fetch firstPage with params: $firstPageRequest.")
      resultSetService.getFirstPage(firstPageRequest)
    case fsPathListAndFirstPageRequest: FsPathListAndFirstPageRequest =>
      logger.info(s"client $sender fetch fsPathListAndFirstPage with params: $fsPathListAndFirstPageRequest.")
      resultSetService.getFsPathListAndFirstPage(fsPathListAndFirstPageRequest)
  }

  // 暂时不考虑超时问题，因为 Linkis1.0 的MessageScheduler已经考虑了
  override def receiveAndReply(message: Any, duration: Duration, sender: Sender): Any = {}

}

class ResultSetCacheReceiverChooser(resultSetService: ResultSetService) extends ReceiverChooser {

  private val receiver: Option[ResultSetCacheReceiver] = Some(new ResultSetCacheReceiver(resultSetService))

  override def chooseReceiver(event: RPCMessageEvent): Option[Receiver] = event.message match {
    case _: ResultSetRequestProtocol => receiver
    case _ => None
  }

}
