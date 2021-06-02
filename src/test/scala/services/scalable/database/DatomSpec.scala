package services.scalable.database

import com.google.common.base.Charsets
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import io.vertx.core.buffer.Buffer
import io.vertx.core.json.{Json, JsonArray}
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.database.grpc._
import services.scalable.index._
import services.scalable.index.impl._

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters.seqAsJavaListConverter

class DatomSpec extends AnyFlatSpec {

  implicit def strToBytes(str: String): Bytes = str.getBytes(Charsets.UTF_8)

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should "serialize and order datoms (serialized as array of arrays of bytes) correctly " in {

    implicit val avetComp = new Ordering[Bytes] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(d0: Bytes, d1: Bytes): Int = {
        /*val c0 = new String(d0, Charsets.UTF_8).split(",")
        val c1 = new String(d1, Charsets.UTF_8).split(",")*/

        comp.compare(d0, d1)
      }
    }

    val NUM_LEAF_ENTRIES = 16
    val NUM_META_ENTRIES = 16

    val indexId = "demo_db"

    implicit val global = ExecutionContext.global
    implicit val cache = new DefaultCache(100L * 1024L * 1024L, 10000)

    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext(indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new QueryableIndex()

    val n = 100

    var datoms = Seq.empty[String]

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

      /*datoms = datoms ++ Seq(
        AVET("person/:name", ByteString.copyFrom(name.getBytes(Charsets.UTF_8)), id, now),
        AVET("person/:age", binAge, id, now),
        AVET("person/:color", ByteString.copyFrom(color), id, now)
      )*/
      datoms = datoms ++ Seq(
        s"person/:name,${name},${id}",
        s"person/:age,${age},${id}",
        s"person/:color,${color},${id}"
      )
    }

    val result = Await.result(index.insert(datoms.map(_ -> EMPTY_ARRAY).map {  case (avet, v) =>
     avet.getBytes(Charsets.UTF_8) -> EMPTY_ARRAY
    }).flatMap(_ => ctx.save()), Duration.Inf)

    logger.info(s"result: ${result}")

    val data = Await.result(index.inOrder(), Duration.Inf).map { case (k, _) =>
      /*val avet = Any.parseFrom(k).unpack(AVET)

      avet.a -> (if(avet.a.compareTo("person/:age") == 0) avet.v.asReadOnlyByteBuffer().getInt()
        else new String(avet.v.toByteArray)) -> avet.e*/
      new String(k, Charsets.UTF_8)
    }

    logger.info(s"${Console.GREEN_B}data: ${data}${Console.RESET}\n")

    val it = index.gt(term = "blue", prefix = Some("person/:color,"))

    def findAll(): Future[Seq[String]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(x => new String(x._1, Charsets.UTF_8)) ++ _}
        }
        case false => Future.successful(Seq.empty[String])
      }
    }

    val r = Await.result(findAll(), Duration.Inf)

    logger.info(s"result: ${r}")

  }

}
