package services.scalable.database

import com.google.common.base.Charsets
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.ByteString
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.database.grpc._
import services.scalable.index._
import services.scalable.index.impl._

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class RangeSpec extends AnyFlatSpec with Repeatable {

  override val times = 1000

  implicit def strToBytes(str: String): Bytes = str.getBytes(Charsets.UTF_8)

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "ranges " should " be equal " in {

    implicit val avetComp = new Ordering[Datom] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(search: Datom, x: Datom): Int = {
        var r: Int = 0

        if(search.a.isDefined && x.a.isDefined){
          r = search.a.get.compareTo(x.a.get)
          if(r != 0) return r
        }

        if(search.v.isDefined && x.v.isDefined){
          r = comp.compare(search.v.get.toByteArray, x.v.get.toByteArray)
          if(r != 0) return r
        }

        if(search.e.isDefined && x.e.isDefined){
          r = search.e.get.compareTo(x.e.get)
          if(r != 0) return r
        }

        if(search.t.isDefined && x.t.isDefined){
          r = search.t.get.compareTo(x.t.get)
          if(r != 0) return r
        }

        r
      }
    }

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    val indexId = "demo_db"

    type K = Datom
    type V = Bytes

    implicit val cache = new DefaultCache[K, V](100L * 1024L * 1024L, 10000)

    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new QueryableIndex[K, V]()

    val n = rand.nextInt(1, 1000)

    var datoms = Seq.empty[Datom]

    val colors = Seq("red", "green", "blue", "magenta", "cyan", "purple", "yellow", "pink")
    val properties = Seq("person/:name", "person/:age", "person/:color")

    for(i<-0 until n){
      val id = RandomStringUtils.randomAlphanumeric(6)
      val name = RandomStringUtils.randomAlphanumeric(6)
      val age = rand.nextInt(18, 1000)
      val now = System.currentTimeMillis()

      val binAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(age).flip())

      val color = colors(rand.nextInt(0, colors.length))

      datoms = datoms ++ Seq(
        Datom(e = Some(id), a = Some("person/:name"), v = Some(ByteString.copyFrom(name.getBytes(Charsets.UTF_8))), t = Some(now)),
        Datom(e = Some(id), a = Some("person/:age"), v = Some(binAge), t = Some(now)),
        Datom(e = Some(id), a = Some("person/:color"), v = Some(ByteString.copyFrom(color)), t = Some(now))
      )
    }

    val result = Await.result(index.insert(datoms.map(_ -> EMPTY_ARRAY).map {  case (avet, _) =>
      avet -> EMPTY_ARRAY
    }).flatMap(_ => ctx.save()), Duration.Inf)

    //logger.info(s"result: ${result}")

    val data = Await.result(index.inOrder(), Duration.Inf).map { case (avet, _) =>
      avet.getA -> (if(avet.getA.compareTo("person/:age") == 0) avet.getV.asReadOnlyByteBuffer().getInt()
        else new String(avet.getV.toByteArray)) -> avet.getE
    }

    logger.info(s"${Console.GREEN_B}data: ${data}${Console.RESET}\n")

    val ageInt = rand.nextInt(18, 1000)
    val minAgeInt = rand.nextInt(18, 1000)
    val maxAgeInt = rand.nextInt(minAgeInt, 1000)

    val ageBin =  ByteString.copyFrom(ByteBuffer.allocate(4).putInt(ageInt).flip())
    val minAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(minAgeInt).flip())
    val maxAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(maxAgeInt).flip())

    datoms = datoms.sorted

    //it.setLimit(5)

    def prettyPrint(list: Seq[Datom]): String = {
      list.map { avet =>
        avet.getA -> avet.getV.asReadOnlyByteBuffer().getInt()
      }.toString()
    }

    def test(it: RichAsyncIterator[Datom, Bytes], cond: Datom => Boolean)(ctx: String): Unit = {

      def findAll(): Future[Seq[Datom]] = {
        it.hasNext().flatMap {
          case true => it.next().flatMap { list =>
            findAll().map{list.map(_._1) ++ _}
          }
          case false => Future.successful(Seq.empty[Datom])
        }
      }

      val result = Await.result(findAll(), Duration.Inf)

      logger.info(ctx)
      //logger.info(s"result: ${result}\n")

      val shouldbe = datoms.filter{_.getA.compareTo("person/:age") == 0}.filter { d =>
        cond(d)
      }

      println("should be: ", prettyPrint(shouldbe))
      println()
      println("calculated: ", prettyPrint(result))

      assert(result == shouldbe)
    }

    var inclusive = rand.nextBoolean()

    test(index.gt(term = Datom(v = Some(ageBin)), prefix = Some(Datom(a = Some("person/:age"))), inclusive = inclusive), d => {
      val age = d.getV.asReadOnlyByteBuffer().getInt()
      inclusive && age >= ageInt || age > ageInt
    })(s"> age: ${ageInt} inclusive: ${inclusive}")

    inclusive = rand.nextBoolean()

    logger.info("\n")

    test(index.lt(term = Datom(v = Some(maxAge)), prefix = Some(Datom(a = Some("person/:age"))), inclusive = inclusive), d => {
      val age = d.getV.asReadOnlyByteBuffer().getInt()
      inclusive && age <= maxAgeInt || age < maxAgeInt
    })(s"< age: ${maxAgeInt} inclusive: ${inclusive}")

    inclusive = rand.nextBoolean()

    logger.info("\n")

    test(index.interval(lowerTerm = Datom(v = Some(minAge)), lowerPrefix = Some(Datom(a = Some("person/:age"))),
      upperTerm = Datom(v = Some(maxAge)), upperPrefix = Some(Datom(a = Some("person/:age"))),
      lowerInclusive = inclusive, upperInclusive = inclusive), d => {
      val age = d.getV.asReadOnlyByteBuffer().getInt()
      (inclusive && age >= minAgeInt || age > minAgeInt) && (inclusive && age <= maxAgeInt || age < maxAgeInt)
    })(s"interval min: ${minAgeInt} max: ${maxAgeInt} inclusive: ${inclusive}")

  }

}
