package services.scalable.database

import services.scalable.database.grpc.Datom
import services.scalable.index.{AsyncIterator, Block, Context, Index}
import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  val $this = this

  def findAll(prefix: K)(implicit prefixOrd: Ordering[K] = this.ord): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V](prefixOrd, ord) {

    val leftMostOrdering = new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = prefixOrd.compare(x, y)

        if(r != 0) return r

        // Here would be 0, but we need to find the first element with the prefix (leftmost one)
        -1
      }
    }

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return findPath(prefix)(leftMostOrdering).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[K, V]]

          case Some(b) =>
            cur = Some(b)

            println(s"\n\nFIRST: ${b.tuples.map{case (k, _) => k.asInstanceOf[Datom].e}}\n\n")

            val filtered = b.tuples.filter{case (k, _) => prefixOrd.equiv(prefix, k)}
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }

      $this.next(cur.map(_.unique_id))(prefixOrd).map {
        case None =>
          cur = None
          Seq.empty[Tuple2[K, V]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.filter{case (k, _) => prefixOrd.equiv(prefix, k)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }

    }

  }

  def gt(term: K, prefix: Option[K] = None, include: Boolean = false): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V](ord, ord) {

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return (if(prefix.isDefined) findPath(prefix.get)(prefixOrd) else findPath(term)(termOrd)).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[K, V]]

          case Some(b) =>
            cur = Some(b)

            println(s"\n\nFIRST GT: ${b.tuples.map{case (k, _) =>
              val d = k.asInstanceOf[Datom]

              d.a -> d.e
            }}\n\n")

            val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && prefixOrd.equiv(prefix.get, k) || prefix.isEmpty) && (include && termOrd.lteq(term, k) || termOrd.lt(term, k))}
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple2[K, V]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && prefixOrd.equiv(prefix.get, k) || prefix.isEmpty) && (include && termOrd.lteq(term, k) || termOrd.lt(term, k))}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }

    }

  }

  def gte(term: K, prefix: Option[K] = None): AsyncIterator[Seq[Tuple2[K, V]]] = gt(term, prefix, true)

  def lt(term: K, prefix: Option[K] = None, include: Boolean = false): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V](ord, ord) {

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return first()(if(prefix.isDefined) prefixOrd else termOrd).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[K, V]]

          case Some(b) =>
            cur = Some(b)
            val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && prefixOrd.equiv(prefix.get, k) || prefix.isEmpty) && (include && termOrd.gteq(term, k) || termOrd.gt(term, k))}
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple2[K, V]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && prefixOrd.equiv(prefix.get, k) || prefix.isEmpty) && (include && termOrd.gteq(term, k) || termOrd.gt(term, k))}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }

    }

  }

  def lte(term: K, prefix: Option[K] = None): AsyncIterator[Seq[Tuple2[K, V]]] = lt(term, prefix, true)

  def interval(lowerTerm: K, upperTerm: K, lowerPrefix: Option[K] = None, upperPrefix: Option[K] = None, includeLower: Boolean = false,
                 includeUpper: Boolean = false): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V](ord, ord) {

      override def hasNext(): Future[Boolean] = synchronized {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      val cond = (k: K) => (lowerPrefix.isDefined && prefixOrd.equiv(lowerPrefix.get, k) || lowerPrefix.isEmpty) && (includeLower && termOrd.lteq(lowerTerm, k) || termOrd.lt(lowerTerm, k)) &&
        (upperPrefix.isDefined && prefixOrd.equiv(upperPrefix.get, k) || upperPrefix.isEmpty) && (includeUpper && termOrd.gteq(upperTerm, k) || termOrd.gt(upperTerm, k))

      val finder = new Ordering[K] {
        override def compare(search: K, y: K): Int = {
          var r = ord.compare(lowerPrefix.get, y)

          if (r != 0) return r

          r = ord.compare(lowerTerm, y)

          if (r > 0) return r

          r
        }
      }

      override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
        if(!firstTime){
          firstTime = true

          return (if(lowerPrefix.isDefined) findPath(lowerPrefix.get)(finder) else findPath(lowerTerm)(termOrd)).map {
            case None =>
              cur = None
              Seq.empty[Tuple2[K, V]]

            case Some(b) =>
              cur = Some(b)

              val filtered = b.tuples.filter{case (k, _) => cond(k) }
              stop = filtered.isEmpty

              checkCounter(filtered.filter{case (k, v) => filter(k, v) })
          }
        }

        $this.next(cur.map(_.unique_id)).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[K, V]]

          case Some(b) =>
            cur = Some(b)

            val filtered = b.tuples.filter{case (k, _) => cond(k)}
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }

      }
  }

}
