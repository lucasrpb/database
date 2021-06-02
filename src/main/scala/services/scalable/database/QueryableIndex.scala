package services.scalable.database

import com.google.common.base.Charsets
import com.google.common.primitives.UnsignedBytes
import services.scalable.index.{AsyncIterator, Block, Bytes, Context, Index}
import com.google.protobuf.any.Any
import services.scalable.database.grpc.AVET

import scala.concurrent.{ExecutionContext, Future}

class QueryableIndex(override implicit val ec: ExecutionContext, override val ctx: Context,
                           implicit val ord: Ordering[Bytes]) extends Index()(ec, ctx) {

  val $this = this

  def gt(term: Bytes, prefix: Option[Bytes] = None): AsyncIterator[Seq[Tuple2[Bytes, Bytes]]] = new AsyncIterator[Seq[(Bytes, Bytes)]] {

    protected var filter: (Bytes, Bytes) => Boolean = (_, _) => true

    protected var cur: Option[Block] = None

    protected var firstTime = false
    protected var stop = false

    val search = if(prefix.isDefined) prefix.get ++ term else term

    override def hasNext(): Future[Boolean] = synchronized {
      if(!firstTime) return Future.successful(ctx.root.isDefined)
      Future.successful(!stop && cur.isDefined)
    }

    protected def hasPrefix(k: Bytes): Boolean = {
      val p = prefix.get
      if(k.length < p.length) return false

      ord.equiv(k.slice(0, p.length), p)
    }

    protected def isGreater(k: Bytes): Boolean = {
      val p = prefix.get

      if(k.length < p.length) return false

      val kterm = k.slice(p.length, k.length)

      ord.gt(kterm, term)
    }

    override def next(): Future[Seq[Tuple2[Bytes, Bytes]]] = synchronized {
      if(!firstTime){
        firstTime = true

        return findPath(search)(ord).map {
          case None =>
            cur = None
            Seq.empty[Tuple2[Bytes, Bytes]]

          case Some(b) =>
            cur = Some(b)

            println(b.tuples.map{case (k, _) => new String(k, Charsets.UTF_8)})

            val filtered = b.tuples.filter{case (k, _) => (prefix.isEmpty || hasPrefix(k)) &&
              (prefix.isEmpty && ord.gt(k, term) || isGreater(k))}
            stop = filtered.isEmpty

            filtered.filter{case (k, v) => filter(k, v)}
        }
      }

      $this.next(cur.map(_.unique_id))(ord).map {
        case None =>
          cur = None
          Seq.empty[Tuple2[Bytes, Bytes]]

        case Some(b) =>
          cur = Some(b)

          val filtered = b.tuples.filter{case (k, _) => (prefix.isEmpty || hasPrefix(k)) && ord.gt(k, term)}
          stop = filtered.isEmpty

          filtered.filter{case (k, v) => filter(k, v) }
      }
    }
  }

}
