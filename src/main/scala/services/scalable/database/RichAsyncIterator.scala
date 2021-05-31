package services.scalable.database

import services.scalable.index.{AsyncIterator, Block, Bytes}

abstract class RichAsyncIterator(protected var prefixOrd: Ordering[Bytes], protected var termOrd: Ordering[Bytes])
  extends AsyncIterator[Seq[Tuple2[Bytes, Bytes]]] {

  protected var limit = Int.MaxValue
  protected var counter = 0

  protected var filter: (Bytes, Bytes) => Boolean = (_, _) => true

  protected var cur: Option[Block] = None

  protected var firstTime = false
  protected var stop = false

  def setPrefixOrdering(ordering: Ordering[Bytes]): Unit = {
    this.prefixOrd = ordering
  }

  def setTermOrdering(ordering: Ordering[Bytes]): Unit = {
    this.termOrd = ordering
  }

  def setLimit(lim: Int): Unit = {
    this.limit = lim
  }

  def setFilter(f: (Bytes, Bytes) => Boolean): Unit = synchronized {
    this.filter = f
  }

  def checkCounter(filtered: Seq[Tuple2[Bytes, Bytes]]): Seq[Tuple2[Bytes, Bytes]] = synchronized {
    val len = filtered.length

    if(counter + len >= limit){
      stop = true
    }

    val n = Math.min(len, limit - counter)

    counter += n

    filtered.slice(0, n)
  }

}
