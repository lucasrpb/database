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
import com.google.protobuf.any.Any

import java.lang.annotation.Repeatable
import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class DatomSpec extends AnyFlatSpec {

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should "serialize and order datoms (serialized as array of arrays of bytes) correctly " in {

    val show = new AtomicBoolean(false)

    implicit val vaetComp = new Ordering[Datom] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(search: Datom, x: Datom): Int = {
        //comp.compare(x.toByteArray, y.toByteArray)

        var r: Int = 0

        if(search.a.isDefined){
          r = search.a.get.compareTo(x.a.get)
          if(r != 0) return r
        }

        if(show.get() && search.a.isDefined){
          logger.info(s"${Console.RED_B}search ${search.a.get} v: ${x.v.get.asReadOnlyByteBuffer().getInt}${Console.RESET}")
        }

        if(search.v.isDefined){
          r = comp.compare(search.v.get.toByteArray, x.v.get.toByteArray)
          if(r != 0) return r
        }

        if(search.e.isDefined){
          r = search.e.get.compareTo(x.e.get)
          if(r != 0) return r
        }

        if(search.t.isDefined){
          r = search.t.get.compareTo(x.t.get)
          if(r != 0) return r
        }

        r
      }
    }

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val indexId = "demo_db"

    implicit val global = ExecutionContext.global
    implicit val cache = new DefaultCache[Datom, Bytes](100L * 1024L * 1024L, 10000)

    implicit val serializer = new GrpcByteSerializer[Datom, Bytes](new Serializer[Datom] {
      override def serialize(t: Datom): Array[Byte] = t.toByteArray
      override def deserialize(b: Array[Byte]): Datom = Any.parseFrom(b).unpack(Datom)
    }, DefaultSerializers.byteSerializer)

   //implicit val storage = new CassandraStorage("indexes", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)

    implicit val storage = new MemoryStorage[Datom, Bytes](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext[Datom, Bytes](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new QueryableIndex[Datom, Array[Byte]]()

    val n = 100

    var datoms = Seq.empty[Datom]

    for(i<-0 until n){
      val id = RandomStringUtils.randomAlphanumeric(5)
      val name = RandomStringUtils.randomAlphanumeric(5)
      val age = rand.nextInt(18, 100)
      val now = System.currentTimeMillis()

      val binAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(age).flip())
      //val binNow = ByteString.copyFrom(ByteBuffer.allocate(4).putLong(now))

      //logger.info(s"to long: ${ByteBuffer.wrap(binAge.toByteArray).getInt}")

      datoms = datoms ++ Seq(
        Datom(Some(id), Some("person/:name"), Some(ByteString.copyFrom(name.getBytes(Charsets.UTF_8))), Some(now)),
        Datom(Some(id), Some("person/:age"), Some(binAge), Some(now))
      )
    }

    val result = Await.result(index.insert(datoms.map(_ -> EMPTY_ARRAY)), Duration.Inf)

    logger.info(s"result: ${result}")

    var data = Await.result(index.inOrder(), Duration.Inf)/*.filter(_._1.a.compareTo("person/:age") == 0)*/
      .map{case (k, _) => s"(${k.a},${k.v.get.asReadOnlyByteBuffer().getInt()},${k.e})"}

    logger.info(s"\n${data}\n")

    /*val gt = new Ordering[Datom] {
      override def compare(x: Datom, y: Datom): Int = {

      }
    }*/

    /*val comp = new Ordering[Datom] {
      val ubc = UnsignedBytes.lexicographicalComparator()

      override def compare(search: Datom, x: Datom): Int = {
        /*var r: Int = 0

        if(search.a.isDefined){
          r = x.a.get.compareTo(search.a.get)
          if(r != 0) return -1
        }

        if(search.v.isDefined){
          r = ubc.compare(x.v.get.toByteArray, search.v.get.toByteArray)

          logger.info(s"${Console.BLUE_B}r: ${r} x ${x.v.get.asReadOnlyByteBuffer().getInt} y: ${search.v.get.asReadOnlyByteBuffer().getInt}${Console.RESET}")

          if(r < 0) return -1
        }

        if(search.e.isDefined){
          r = x.e.get.compareTo(search.e.get)
          if(r != 0) return -1
        }

        if(search.t.isDefined){
          r = x.t.get.compareTo(search.t.get)
        }

        if(r < 0) return -1

        r*/

        var r: Int = 0

        if(search.a.isDefined){
          r = search.a.get.compareTo(x.a.get)
          if(r != 0) return r
        }

        if(search.v.isDefined){
          r = ubc.compare(x.v.get.toByteArray, search.v.get.toByteArray)

          if(show.get()){
            logger.info(s"${Console.RED_B}search ${search.v.get.asReadOnlyByteBuffer().getInt} x: ${x.v.get.asReadOnlyByteBuffer().getInt}${Console.RESET}")
          }

          if(r < 0) return r
        }

        if(search.e.isDefined){
          r = search.e.get.compareTo(x.e.get)
          if(r != 0) return r
        }

        if(search.t.isDefined){
          r = search.t.get.compareTo(x.t.get)
          if(r != 0) return r
        }

        0
      }
    }*/

    show.set(true)

    val prefix = Datom(a = Some("person/:age"))

    //val it = index.gt(Datom(v = Some(age)), prefix = Some(Datom(a = Some("person/:age"))))
    //val it = index.gt(Datom(a = Some("person/:age"), v = Some(age)))

    //val it = index.lte(Datom(v = Some(age)), prefix = Some(Datom(a = Some("person/:age"))))

    val age = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(70).flip())
    val minAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(18).flip())
    val maxAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(50).flip())

    val it = index.interval(lowerTerm = Datom(v = Some(minAge)), upperTerm = Datom(v = Some(maxAge)), lowerPrefix = Some(prefix), upperPrefix = Some(prefix))

    def getAll(): Future[Seq[Datom]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          getAll().map{list.map(_._1) ++ _}
        }
        case false => Future.successful(Seq.empty[Datom])
      }
    }

    val r = Await.result(getAll(), Duration.Inf)

    //logger.info(s"${Await.result(index.find(Datom(a = Some("person/:age"))), Duration.Inf).map(_._1)}")

    logger.info(s"\n > 70 : ${r.map{k => s"(${k.a},${k.v.get.asReadOnlyByteBuffer().getInt()},${k.e})"}}")
  }

}
