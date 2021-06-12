package services.scalable.database

import services.scalable.database.grpc.Datom
import services.scalable.index.{Block, Context, Index, Leaf, Meta, Tuple}

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex[K, V]()(override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index()(ec, ctx) {

  override val $this = this

  override def findPath(k: K, start: Block[K,V], limit: Option[Block[K,V]])(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {

    if(limit.isDefined && limit.get.unique_id.equals(start.unique_id)){
      logger.debug(s"reached limit!")
      return Future.successful(None)
    }

    start match {
      case leaf: Leaf[K,V] => Future.successful(Some(leaf))
      case meta: Meta[K,V] =>

        meta.setPointers()(ctx)

        val bid = meta.findPath(k)

        ctx.get(bid).flatMap { block =>
          findPath(k, block, limit)
        }
    }
  }

  override def findPath(k: K, limit: Option[Block[K,V]] = None)(implicit ord: Ordering[K]): Future[Option[Leaf[K,V]]] = {
    if(ctx.root.isEmpty) {
      return Future.successful(None)
    }

    val bid = ctx.root.get

    ctx.get(ctx.root.get).flatMap { start =>
      ctx.setParent(bid, 0, None)
      findPath(k, start, limit)
    }
  }

  def gt(term: K, inclusive: Boolean = false)(implicit prefixOrder: Ordering[K], termOrdering: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = termOrdering.compare(term, y)

        if(r != 0) return r

        if(inclusive) -1 else 1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    def check(k: K): Boolean = {
      prefixOrder.equiv(k, k) && (inclusive && termOrdering.gteq(k, term) || termOrdering.gt(k, term))
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

      $this.next(cur.map(_.unique_id))(termOrdering).map {
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

  def lt(term: K, inclusive: Boolean = false)(implicit prefixOrder: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val ltOrd = new Ordering[K] {
      override def compare(x: K, y: K): Int = {
        val r = prefixOrder.compare(y, y)

        if(r != 0) return r

        -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = {
      prefixOrder.equiv(k, k) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
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

      $this.next(cur.map(_.unique_id))(termOrd).map {
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
              (implicit lowerPrefixOrd: Ordering[K], upperPrefixOrd: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = termOrd.compare(lowerTerm, y)

        if(r != 0) return r

        if(lowerInclusive) -1 else 1
      }
    }

      override def hasNext(): Future[Boolean] = {
        if(!firstTime) return Future.successful(ctx.root.isDefined)
        Future.successful(!stop && cur.isDefined)
      }

      def check(k: K): Boolean = {
        (lowerPrefixOrd.equiv(k, k)) && (lowerInclusive && termOrd.gteq(k, lowerTerm) || termOrd.gt(k, lowerTerm)) &&
          (upperPrefixOrd.equiv(k, k) && (upperInclusive && termOrd.lteq(k, upperTerm) || termOrd.lt(k, upperTerm)))
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

        $this.next(cur.map(_.unique_id))(termOrd).map {
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

  def ltr(term: K, inclusive: Boolean = false)(implicit prefixOrder: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = termOrd.compare(x, y)

        if(r != 0) return r

        if(inclusive) 1 else -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = {
      prefixOrder.equiv(k, k) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
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

            println("FIRST:\n")

            println(b.tuples.map { case (k, _) =>
              val d = k.asInstanceOf[Datom]
              d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
            })

            println()

            val filtered = b.tuples.reverse.filter{case (k, _) => check(k) }
            //stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
        }
      }

      $this.prev(cur.map(_.unique_id))(termOrd).map {
        case None =>
          cur = None
          Seq.empty[Tuple[K, V]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.reverse.filter{case (k, v) => check(k)}
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

  def gtr(term: K, inclusive: Boolean = false)(implicit prefixOrder: Ordering[K], termOrdering: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = prefixOrder.compare(term, y)

        // Iterates until the last subtree with the prefix.
        if(r != 0) return r

        //Arrives at the first block after the prefix.
        1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    def check(k: K): Boolean = {
      prefixOrder.equiv(k, k) && (inclusive && termOrdering.gteq(k, term) || termOrdering.gt(k, term))
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

            println("FIRST: \n")

            println(b.tuples.map { case (k, _) =>
              val d = k.asInstanceOf[Datom]
              d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
            })

            // println(b.tuples.map(_._1))

            println(b.tuples.filter{case (k, _) => check(k) })

            val filtered = b.tuples.reverse.filter{case (k, _) => check(k) }
            //stop = filtered.isEmpty

            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
        }
      }

      $this.prev(cur.map(_.unique_id))(termOrdering).map {
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

          println(b.tuples.filter{case (k, _) => check(k) })*/

          val filtered = b.tuples.reverse.filter{case (k, _) => check(k) }
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

  def intervalr(lowerTerm: K, upperTerm: K, lowerInclusive: Boolean = true, upperInclusive: Boolean = true)
              (implicit lowerPrefixOrd: Ordering[K], upperPrefixOrd: Ordering[K], termOrd: Ordering[K]): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = termOrd.compare(upperTerm, y)

        if(r != 0) return r

        if(upperInclusive) 1 else -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    def check(k: K): Boolean = {
      (lowerPrefixOrd.equiv(k, k)) && (lowerInclusive && termOrd.gteq(k, lowerTerm) || termOrd.gt(k, lowerTerm)) &&
        (upperPrefixOrd.equiv(k, k) && (upperInclusive && termOrd.lteq(k, upperTerm) || termOrd.lt(k, upperTerm)))
    }

    override def next(): Future[Seq[Tuple[K, V]]] = {
      if(!firstTime){
        firstTime = true

        return findPath(upperTerm)(searchOrd).map {
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

            // println(b.tuples.map(_._1))

            println()*/

            val filtered = b.tuples.reverse.filter{case (k, _) => check(k) }

            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
        }
      }

      $this.prev(cur.map(_.unique_id))(termOrd).map {
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

          val filtered = b.tuples.reverse.filter{case (k, _) => check(k) }
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }

  }

}
