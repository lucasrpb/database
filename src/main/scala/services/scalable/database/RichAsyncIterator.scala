package services.scalable.database

import services.scalable.index.{AsyncIterator, Block}

abstract class RichAsyncIterator[K, V](protected var prefixOrd: Ordering[K], protected var termOrd: Ordering[K]) extends AsyncIterator[Seq[Tuple2[K, V]]] {

  protected var limit = Int.MaxValue
  protected var counter = 0

  protected var filter: (K, V) => Boolean = (_, _) => true

  protected var cur: Option[Block[K, V]] = None

  protected var firstTime = false
  protected var stop = false

  def setPrefixOrdering(ordering: Ordering[K]): Unit = {
    this.prefixOrd = ordering
  }

  def setTermOrdering(ordering: Ordering[K]): Unit = {
    this.termOrd = ordering
  }

  def setLimit(lim: Int): Unit = {
    this.limit = lim
  }

  def setFilter(f: (K, V) => Boolean): Unit = synchronized {
    this.filter = f
  }

  def checkCounter(filtered: Seq[Tuple2[K, V]]): Seq[Tuple2[K, V]] = synchronized {
    val len = filtered.length

    if(counter + len >= limit){
      stop = true
    }

    val n = Math.min(len, limit - counter)

    counter += n

    filtered.slice(0, n)
  }

}
