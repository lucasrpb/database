package services.scalable.database

import services.scalable.index.{Context, Index, Tuple}

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index()(ec, ctx) {

  val $this = this

  def gt(term: K, inclusive: Boolean = false)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    override def next(): Future[Seq[Tuple[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return findPath(term)(ord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, v) => inclusive && ord.gteq(k, term) || ord.gt(k, term)}
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.filter{case (k, v) => inclusive && ord.gteq(k, term) || ord.gt(k, term)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

}
