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

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

class DatomSpec extends AnyFlatSpec {

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should "serialize and order datoms (serialized as array of arrays of bytes) correctly " in {

    implicit val vaetComp = new Ordering[Datom] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(x: Datom, y: Datom): Int = {
        //comp.compare(x.toByteArray, y.toByteArray)

        var r = x.a.compareTo(y.a)

        if(r != 0) return r

        r = comp.compare(x.v.toByteArray, y.v.toByteArray)

        if(r != 0) return r

        r = x.e.compareTo(y.e)

        if(r != 0) return r

        x.t.compareTo(y.t)
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

      logger.info(s"to long: ${ByteBuffer.wrap(binAge.toByteArray).getInt}")

      datoms = datoms ++ Seq(
        Datom(id, "person/:name", ByteString.copyFrom(name.getBytes(Charsets.UTF_8)), now),
        Datom(id, "person/:age", binAge, now)
      )
    }

    val result = Await.result(index.insert(datoms.map(_ -> EMPTY_ARRAY)), Duration.Inf)

    logger.info(s"result: ${result}")

    val data = Await.result(index.inOrder(), Duration.Inf).filter(_._1.a.compareTo("person/:age") == 0)
      .map{case (k, _) => s"(${k.a},${k.v.asReadOnlyByteBuffer().getInt()},${k.e})"}

    logger.info(s"\n${data}\n")
  }

}
