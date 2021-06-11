package services.scalable.database

import ch.qos.logback.classic.LoggerContext
import com.google.common.base.Charsets
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.database.grpc.Datom
import services.scalable.index.Bytes
import services.scalable.index.impl.{DefaultCache, DefaultContext, MemoryStorage}

import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class IntSpec extends  AnyFlatSpec with Repeatable {

  override val times = 1000

  implicit val ord = new Ordering[Int] {
    override def compare(x: Int, y: Int): Int = {
      x.compareTo(y)
    }
  }

  implicit def strToBytes(str: String): Bytes = str.getBytes(Charsets.UTF_8)

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  //val logger = LoggerFactory.getLogger(this.getClass)
  val factory = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  val logger = factory.getLogger(this.getClass)

  "btree result query " should " be equal " in {

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    val indexId = "demo_db"

    type K = Int
    type V = Int

    implicit val cache = new DefaultCache[K, V](100L * 1024L * 1024L, 10000)

    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new QueryableIndex()

    val n = rand.nextInt(1, 1000)

    var datoms = Seq.empty[Int]

    for(i<-0 until n){
      val num = rand.nextInt(0, 10000)

      if(!datoms.exists{_ == num}){
        datoms = datoms :+ num
      }
    }

    Await.result(index.insert(datoms.map{_ -> 0}).flatMap(_ => ctx.save()), Duration.Inf)

    val allit = index.inOrder()

    val idata = Await.result(TestHelper.all(allit), Duration.Inf).map{case (k, _) => k}
    val sorted = datoms.sorted

    assert(idata == sorted)

    val inclusive = rand.nextBoolean()
    val upperInclusive = rand.nextBoolean()

    val term = rand.nextInt(0, 10000)
    val upperTerm = rand.nextInt(term, 10000)

    val it = index.gt(term = term, inclusive = inclusive)
    /*val it = index.lt(term = term, prefix = None, inclusive = inclusive)*/
   /* val it = index.interval(term, upperTerm, None, None, inclusive, upperInclusive)*/

    //it.setLimit(5)

    def checkLt(k: K): Boolean = {
      (inclusive && ord.lteq(k, term) || ord.lt(k, term))
    }

    def checkGt(k: K): Boolean = {
      (inclusive && ord.gteq(k, term) || ord.gt(k, term))
    }

    def checkInterval(k: K): Boolean = {
        (inclusive && ord.gteq(k, term) || ord.gt(k, term)) &&
        (upperInclusive && ord.lteq(k, upperTerm) || ord.lt(k, upperTerm))
    }

    def findAll(): Future[Seq[Int]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(_._1) ++ _}
        }
        case false => Future.successful(Seq.empty[Int])
      }
    }

    val gtlist = Await.result(findAll(), Duration.Inf)
    val list = sorted.filter{e => checkInterval(e)}

    logger.info(s"\nindex query lowterm ${term} upperterm ${upperTerm} inclusive $inclusive upperInclusive ${upperInclusive}: ${gtlist}\n")
    logger.info(s"\nindex query lowterm ${term} upperterm ${upperTerm} inclusive $inclusive upperInclusive ${upperInclusive}: ${list}\n")

    assert(gtlist == list)
  }

}
