package services.scalable.database

import org.checkerframework.checker.units.qual.Prefix
import services.scalable.database.grpc.Datom
import services.scalable.index.{Block, Context, Index, Leaf, Meta, Tuple}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V],
                           implicit val ord: Ordering[K]) extends Index()(ec, ctx) {

  val $this = this

  def gt(term: K, prefix: Option[K] = None, inclusive: Boolean = false): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = ord.compare(x, y)

        if(r != 0) return r

        -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    def check(k: K): Boolean = {
      (prefix.isEmpty || ord.equiv(prefix.get, k)) && (inclusive && ord.gteq(k, term) || ord.gt(k, term))
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

            /*println(b.tuples.map { case (k, _) =>
              val d = k.asInstanceOf[Datom]
              d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
            })*/

           // println(b.tuples.map(_._1))

            println()

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            if(!inclusive && filtered.isEmpty && b.tuples.exists{case(k, _) => ord.equiv(k, term)}){
              stop = false
            }

            checkCounter(filtered.filter{case (k, v) => filter(k, v)})
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

  def lt(term: K, prefix: Option[K] = None, inclusive: Boolean = false): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val ltOrd = new Ordering[K] {
      override def compare(x: K, y: K): Int = {

        if(prefix.isDefined){
          val r = ord.compare(prefix.get, y)
          if(r != 0) return r
        }

       -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def check(k: K): Boolean = {
      (prefix.isEmpty || ord.equiv(prefix.get, k)) && (inclusive && ord.lteq(k, term) || ord.lt(k, term))
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

            if(!inclusive && filtered.isEmpty && b.tuples.exists{case(k, _) => ord.equiv(k, term)}){
              stop = false
            }

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

  def interval(lowerTerm: K, upperTerm: K, lowerPrefix: Option[K] = None, upperPrefix: Option[K],
               lowerInclusive: Boolean = true, upperInclusive: Boolean = true): RichAsyncIterator[K, V] = new RichAsyncIterator[K, V] {

    val searchOrd = new Ordering[K]{
      override def compare(x: K, y: K): Int = {
        val r = ord.compare(x, y)

        if(r != 0) return r

        -1
      }
    }

    override def hasNext(): Future[Boolean] = {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    def check(k: K): Boolean = {
      (lowerPrefix.isEmpty || ord.gteq(lowerPrefix.get, lowerTerm)) &&
        (upperPrefix.isEmpty || ord.lteq(upperPrefix.get, upperTerm)) &&
      (lowerInclusive && ord.gteq(k, lowerTerm) || ord.gt(k, lowerTerm)) &&
        (upperInclusive && ord.lteq(k, upperTerm) || ord.lt(k, upperTerm))
    }

    override def next(): Future[Seq[Tuple[K, V]]] = {
      if(!firstTime){
        firstTime = true

        return findPath(lowerTerm)(searchOrd).map {
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

             //println(b.tuples.map(_._1))

            println()*/

            val filtered = b.tuples.filter{case (k, _) => check(k) }
            stop = filtered.isEmpty

            if(!lowerInclusive && filtered.isEmpty && b.tuples.exists{case(k, _) => ord.equiv(k, lowerTerm)} ||
              !upperInclusive && filtered.isEmpty && b.tuples.exists{case (k, _) => ord.equiv(k, upperTerm)}){
              stop = false
            }

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

          println(b.tuples.map(_._1))

          println()*/

          val filtered = b.tuples.filter{case (k, _) => check(k) }
          stop = filtered.isEmpty

          checkCounter(filtered.filter{case (k, v) => filter(k, v) })
      }
    }
  }

}
