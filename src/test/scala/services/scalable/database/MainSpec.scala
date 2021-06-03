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

class MainSpec extends AnyFlatSpec {

  implicit def strToBytes(str: String): Bytes = str.getBytes(Charsets.UTF_8)

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should "serialize and order datoms (serialized as array of arrays of bytes) correctly " in {

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

    val n = 100

    var datoms = Seq.empty[Datom]

    val colors = Seq("red", "green", "blue", "magenta", "cyan", "purple", "yellow", "pink")

    for(i<-0 until n){
      val id = RandomStringUtils.randomAlphanumeric(5)
      val name = RandomStringUtils.randomAlphanumeric(5)
      val age = rand.nextInt(18, 100)
      val now = System.currentTimeMillis()

      val binAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(age).flip())
      //val binNow = ByteString.copyFrom(ByteBuffer.allocate(4).putLong(now))

      //logger.info(s"to long: ${ByteBuffer.wrap(binAge.toByteArray).getInt}")

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

    logger.info(s"result: ${result}")

    val data = Await.result(index.inOrder(), Duration.Inf).map { case (avet, _) =>
      avet.getA -> (if(avet.getA.compareTo("person/:age") == 0) avet.getV.asReadOnlyByteBuffer().getInt()
        else new String(avet.getV.toByteArray)) -> avet.getE
    }

    logger.info(s"${Console.GREEN_B}data: ${data}${Console.RESET}\n")

    val age =  ByteString.copyFrom(ByteBuffer.allocate(4).putInt(70).flip())
    val minAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(20).flip())
    val maxAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(50).flip())

    //val it = index.gt(term = Datom(v = Some(age)), prefix = Some(Datom(a = Some("person/:age"))), inclusive = true)
    //val it = index.gt(Datom(a = Some("person/:color")), inclusive = true)

    //val it = index.lt(term = Datom(v = Some(age)), prefix = Some(Datom(a = Some("person/:age"))), inclusive = true)
    //val it = index.lt(Datom(a = Some("person/:color"), v = Some(age)), inclusive = true)

    /*val it = index.interval(lowerTerm = Datom(v = Some(minAge)), upperTerm = Datom(v = Some(maxAge)),
      lowerPrefix = Some(Datom(a = Some("person/:age"))), upperPrefix = Some(Datom(a = Some("person/:age"))))*/

    val it = index.interval(lowerTerm = Datom(v = Some(minAge)), upperTerm = Datom(v = Some(ByteString.copyFrom("blue".getBytes(Charsets.UTF_8)))),
      lowerPrefix = Some(Datom(a = Some("person/:age"))), upperPrefix = Some(Datom(a = Some("person/:color"))))

    //it.setLimit(5)

    def findAll(): Future[Seq[Datom]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(_._1) ++ _}
        }
        case false => Future.successful(Seq.empty[Datom])
      }
    }

    val r = Await.result(findAll(), Duration.Inf).map { avet =>
      avet.getA -> (if(avet.getA.compareTo("person/:age") == 0) avet.getV.asReadOnlyByteBuffer().getInt()
      else new String(avet.getV.toByteArray)) -> avet.getE
    }

    logger.info(s"\nresult: ${r}")

  }

}
