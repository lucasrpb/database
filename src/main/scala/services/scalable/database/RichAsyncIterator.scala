package services.scalable.database

import services.scalable.index.{AsyncIterator, Block, Bytes, Tuple}

abstract class RichAsyncIterator[K, V]() extends AsyncIterator[Seq[Tuple[K, V]]] {

  protected var limit = Int.MaxValue
  protected var counter = 0

  protected var filter: (K, V) => Boolean = (_, _) => true

  protected var cur: Option[Block[K, V]] = None

  protected var firstTime = false
  protected var stop = false

  def setLimit(lim: Int): Unit = {
    this.limit = lim
  }

  def setFilter(f: (K, V) => Boolean): Unit = synchronized {
    this.filter = f
  }

  def checkCounter(filtered: Seq[Tuple[K, V]]): Seq[Tuple2[K, V]] = synchronized {
    val len = filtered.length

    if(counter + len >= limit){
      stop = true
    }

    val n = Math.min(len, limit - counter)

    counter += n

    filtered.slice(0, n)
  }

}
