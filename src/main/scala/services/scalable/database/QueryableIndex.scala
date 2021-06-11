package services.scalable.database

import services.scalable.database.grpc.Datom
import services.scalable.index.{Context, Index, Tuple}

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index()(ec, ctx) {

  override val $this = this

  def gt(term: K, inclusive: Boolean = false)(implicit prefixOrder: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = ord.compare(term, y)

        if(r != 0) return r

        -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    def check(k: K): Boolean = {
      prefixOrder.equiv(k, k) && (inclusive && ord.gteq(k, term) || ord.gt(k, term))
    }

    override def next(): Future[Seq[Tuple[K, V]]] = {
      if(!firstTime){
        firstTime = true

        return findPath(term)(searchOrd).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            println()

            println(b.tuples.map { case (k, _) =>
              val d = k.asInstanceOf[Datom]
              d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
            })

           // println(b.tuples.map(_._1))

            println()

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            /**
             * Exception in the case of having only the term left in the block because of the
             * inclusive flag been set to false. This way we miss out the next elements.
             */
            /*if(!inclusive && filtered.isEmpty && b.tuples.exists{case(k, _) => termOrdering.equiv(k, term)}){
              stop = false
            }*/

            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)

          /*println()

          println(b.tuples.map { case (k, _) =>
            val d = k.asInstanceOf[Datom]
            d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
          })

          println()*/

          val filtered = b.tuples.filter{case (k, _) => check(k) }
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

  def lt(term: K, inclusive: Boolean = false)(implicit prefixOrder: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val ltOrd = new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = prefixOrder.compare(x, y)

        if(r != 0) return r

        -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = {
      prefixOrder.equiv(k, k) && (inclusive && ord.lteq(k, term) || ord.lt(k, term))
    }

    override def next(): Future[Seq[Tuple[K, V]]] = {
      if(!firstTime){
        firstTime = true

        return findPath(term)(ltOrd).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            /*println()

            println(b.tuples.map { case (k, _) =>
              val d = k.asInstanceOf[Datom]
              d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
            })

            println()*/

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

          val filtered = b.tuples.filter{case (k, v) => check(k)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

  def interval(lowerTerm: K, upperTerm: K, lowerInclusive: Boolean = true, upperInclusive: Boolean = true)
              (implicit lowerPrefixOrd: Ordering[K], upperPrefixOrd: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = ord.compare(lowerTerm, y)

        if(r != 0) return r

        -1
      }
    }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (lowerPrefixOrd.equiv(k, k)) && (lowerInclusive && ord.gteq(k, lowerTerm) || ord.gt(k, lowerTerm)) &&
          (upperPrefixOrd.equiv(k, k) && (upperInclusive && ord.lteq(k, upperTerm) || ord.lt(k, upperTerm)))
      }

      override def next(): Future[Seq[Tuple[K, V]]] = {
        if(!firstTime){
          firstTime = true

          return findPath(lowerTerm)(searchOrd).flatMap {
            case None =>
              cur = None
              Future.successful(Seq.empty[Tuple[K, V]])

            case Some(b) =>
              cur = Some(b)

              println()

              println(b.tuples.map { case (k, _) =>
                val d = k.asInstanceOf[Datom]
                d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
              })

              // println(b.tuples.map(_._1))

              println()

              val filtered = b.tuples.filter{case (k, _) => check(k) }
              stop = filtered.isEmpty

              Future.successful(checkCounter(filtered.filter{case (k, v) => filter(k, v)}))
          }
        }

        $this.next(cur.map(_.unique_id))(ord).map {
          case None =>
            cur = None
            Seq.empty[Tuple[K, V]]

          case Some(b) =>
            cur = Some(b)

            println()

            println(b.tuples.map { case (k, _) =>
              val d = k.asInstanceOf[Datom]
              d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
            })

            println()

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v) })
        }
      }

  }

}
