package services.scalable.database

import services.scalable.index.{AsyncIterator, Block, Context, Index}
import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index[K, V]()(ec, ctx) {

  val $this = this

  def gt(term: K, prefix: Option[K] = None, include: Boolean = false, limit: Int = Int.MaxValue, filter: (K, V) => Boolean = (k, v) => true)
        /*(implicit ord: Ordering[K])*/: AsyncIterator[Seq[Tuple2[K, V]]] = new AsyncIterator[Seq[Tuple2[K, V]]] {

    private var cur: Option[Block[K, V]] = None
    private var firstTime = false
    private var stop = false
    private var counter = 0

    def checkCounter(filtered: Seq[Tuple2[K, V]]): Seq[Tuple2[K, V]] = {
      val len = filtered.length

      if(counter + len >= limit){
        stop = true
      }

      val n = Math.min(len, limit - counter)

      counter += n

      filtered.slice(0, n)
    }

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return findPath(if(prefix.isDefined) prefix.get else term).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[K, V]]

          case Some(b) =>
            cur = Some(b)
            val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && ord.equiv(prefix.get, k) || prefix.isEmpty) && (include && ord.lteq(term, k) || ord.lt(term, k))}
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

          val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && ord.equiv(prefix.get, k) || prefix.isEmpty) && (include && ord.lteq(term, k) || ord.lt(term, k))}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }

    }

  }

  def gte(term: K, prefix: Option[K] = None, limit: Int = Int.MaxValue, filter: (K, V) => Boolean = (k, v) => true): AsyncIterator[Seq[Tuple2[K, V]]] =
    gt(term, prefix, true, limit, filter)

  def lt(term: K, prefix: Option[K] = None, include: Boolean = false, limit: Int = Int.MaxValue, filter: (K, V) => Boolean = (k, v) => true):
    AsyncIterator[Seq[Tuple2[K, V]]] = new AsyncIterator[Seq[Tuple2[K, V]]] {

    private var cur: Option[Block[K, V]] = None
    private var firstTime = false
    private var stop = false
    private var counter = 0

    def checkCounter(filtered: Seq[Tuple2[K, V]]): Seq[Tuple2[K, V]] = {
      val len = filtered.length

      if(counter + len >= limit){
        stop = true
      }

      val n = Math.min(len, limit - counter)

      counter += n

      filtered.slice(0, n)
    }

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return first()(ord).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[K, V]]

          case Some(b) =>
            cur = Some(b)
            val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && ord.equiv(prefix.get, k) || prefix.isEmpty) && (include && ord.gteq(term, k) || ord.gt(term, k))}
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

          val filtered = b.tuples.filter{case (k, _) => (prefix.isDefined && ord.equiv(prefix.get, k) || prefix.isEmpty) && (include && ord.gteq(term, k) || ord.gt(term, k))}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }

    }

  }

  def lte(term: K, prefix: Option[K] = None, limit: Int = Int.MaxValue, filter: (K, V) => Boolean = (k, v) => true): AsyncIterator[Seq[Tuple2[K, V]]] =
    lt(term, prefix, true, limit, filter)

  def interval(lowerTerm: K, upperTerm: K, lowerPrefix: Option[K] = None, upperPrefix: Option[K] = None, includeLower: Boolean = false,
                 includeUpper: Boolean = false, limit: Int = Int.MaxValue, filter: (K, V) => Boolean = (k, v) => true): AsyncIterator[Seq[Tuple2[K, V]]] = new AsyncIterator[Seq[(K, V)]] {
      private var cur: Option[Block[K, V]] = None
      private var firstTime = false
      private var stop = false
      private var counter = 0

      override def hasNext(): Future[Boolean] = synchronized {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      val cond = (k: K) => (lowerPrefix.isDefined && ord.equiv(lowerPrefix.get, k) || lowerPrefix.isEmpty) && (includeLower && ord.lteq(lowerTerm, k) || ord.lt(lowerTerm, k)) &&
        (upperPrefix.isDefined && ord.equiv(upperPrefix.get, k) || upperPrefix.isEmpty) && (includeUpper && ord.gteq(upperTerm, k) || ord.gt(upperTerm, k))

      val finder = new Ordering[K] {
        override def compare(search: K, y: K): Int = {
          var r = ord.compare(lowerPrefix.get, y)

          if (r != 0) return r

          r = ord.compare(lowerTerm, y)

          if (r > 0) return r

          r
        }
      }

      def checkCounter(filtered: Seq[Tuple2[K, V]]): Seq[Tuple2[K, V]] = {
        val len = filtered.length

        if(counter + len >= limit){
          stop = true
        }

        val n = Math.min(len, limit - counter)

        counter += n

        filtered.slice(0, n)
      }

      override def next(): Future[Seq[Tuple2[K, V]]] = synchronized {
        if(!firstTime){
          firstTime = true

          return (if(lowerPrefix.isDefined) findPath(lowerPrefix.get)(finder) else findPath(lowerTerm)(ord)).map {
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
