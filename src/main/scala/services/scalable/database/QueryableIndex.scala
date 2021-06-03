package services.scalable.database

import services.scalable.database.grpc.Datom
import services.scalable.index.{Context, Index, Tuple}

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index()(ec, ctx) {

  val $this = this

  def gt(term: K, prefix: Option[K] = None, inclusive: Boolean = false)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K] {
      override def compare(x: K, y: K): Int = {

        if(prefix.isDefined){
          val r = ord.compare(prefix.get, y)
          if(r != 0) return r
        }

        val r = ord.compare(term, y)

        /*
          * If not inclusive we can fall into an exceptional case where the stop flag is set to true even when elements
          *  to iterate on the next block! So we jump straight to it.
         */
        if(!inclusive && r == 0) 1 else r
      }
    }

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = (prefix.isEmpty || ord.equiv(prefix.get, k)) && (inclusive && ord.gteq(k, term) || ord.gt(k, term))

    override def next(): Future[Seq[Tuple[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return findPath(term)(searchOrd).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.filter{case (k, v) => check(k)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

  def lt(term: K, prefix: Option[K] = None, inclusive: Boolean = false)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val ordering = new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = ord.compare(x, y)

        // We need to know where to go...
        if(r != 0) return r

        // Ensures to get the immediately previous block that could contain the prefix.
        -1
      }
    }

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = (prefix.isEmpty || ord.equiv(prefix.get, k)) && (inclusive && ord.lteq(k, term) || ord.lt(k, term))

    override def next(): Future[Seq[Tuple[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        /**
         * If a prefix is provided, we must find the first node with the prefix and iterate from there. Otherwise,
         * we must start from the first block in the tree (there are an "infinite" number of keys before any key)
         */
        return (if(prefix.isDefined) findPath(prefix.get)(ordering) else first()(ord)).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            /*println(b.tuples.map(_._1.asInstanceOf[Datom]).map{ d =>
              d.getA -> d.getE -> d.getV.asReadOnlyByteBuffer().getInt()
            })*/

            val filtered = b.tuples.filter{case (k, _) => check(k) }
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

          val filtered = b.tuples.filter{case (k, _) => check(k)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

  def interval(lowerTerm: K, upperTerm: K, lowerPrefix: Option[K] = None, upperPrefix: Option[K] = None,
               lowerInclusive: Boolean = true, upperInclusive: Boolean = true)(implicit ord: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K] {
      override def compare(x: K, y: K): Int = {

        if(lowerPrefix.isDefined){
          val r = ord.compare(lowerPrefix.get, y)
          if(r != 0) return r
        }

        val r = ord.compare(lowerTerm, y)

        /*
          * If not inclusive we can fall into an exceptional case where the stop flag is set to true even when elements
          *  to iterate on the next block! So we jump straight to it.
         */
        if(!lowerInclusive && r == 0) 1 else r
      }
    }

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = ((lowerPrefix.isEmpty || ord.equiv(lowerPrefix.get, k)) && (lowerInclusive && ord.gteq(k, lowerTerm)) || ord.gt(k, lowerTerm)) &&
      ((upperPrefix.isEmpty || ord.equiv(upperPrefix.get, k)) && (upperInclusive && ord.lteq(k, upperTerm)) || ord.lt(k, upperTerm))

    override def next(): Future[Seq[Tuple[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return findPath(lowerTerm)(searchOrd).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            /*println(b.tuples.map(_._1.asInstanceOf[Datom]).map{ d =>
              d.getA -> d.getE -> d.getV.asReadOnlyByteBuffer().getInt()
            })*/

            val filtered = b.tuples.filter{case (k, v) => check(k)}
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

          val filtered = b.tuples.filter{case (k, v) => check(k)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

}
