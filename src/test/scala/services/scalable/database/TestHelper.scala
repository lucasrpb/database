package services.scalable.database

import services.scalable.index.{AsyncIterator, Tuple}
import scala.concurrent.{ExecutionContext, Future}

object TestHelper {

  def all[K, V](it: AsyncIterator[Seq[Tuple[K, V]]])(implicit ec: ExecutionContext): Future[Seq[Tuple[K, V]]] = {
    it.hasNext().flatMap {
      case true => it.next().flatMap { list =>
        all(it).map{list ++ _}
      }
      case false => Future.successful(Seq.empty[Tuple[K, V]])
    }
  }

}
