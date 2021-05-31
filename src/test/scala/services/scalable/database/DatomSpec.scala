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

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  val logger = LoggerFactory.getLogger(this.getClass)

  "it " should "serialize and order datoms (serialized as array of arrays of bytes) correctly " in {

    val show = new AtomicBoolean(false)

    def datomToBytes(k: Datom): Array[Byte] = {

      /*var list = Seq.empty[Object]

      if(k.a.isDefined){
        list = list :+ k.a.get
      }

      if(k.v.isDefined){
        list = list :+ k.v.get.toByteArray
      }

      if(k.e.isDefined){
        list = list :+ k.e.get
      }

      if(k.t.isDefined){
        list = list :+ k.t.get.asInstanceOf[Object]
      }

      val arr = new JsonArray(
        list.asJava
      )

      arr.toBuffer.getBytes*/

      val buf = Buffer.buffer()

      if(k.a.isDefined){
        val bytes = k.a.get.getBytes(Charsets.UTF_8)

        buf.appendInt(bytes.length)
        buf.appendBytes(bytes)
      }

      if(k.v.isDefined){
        val bytes = k.v.get.toByteArray

        buf.appendInt(bytes.length)
        buf.appendBytes(bytes)
      }

      if(k.e.isDefined){
        val bytes = k.e.get.getBytes(Charsets.UTF_8)

        buf.appendInt(bytes.length)
        buf.appendBytes(bytes)
      }

      if(k.t.isDefined){
        val bytes = ByteBuffer.allocate(8).putLong(k.t.get).array()

        buf.appendInt(bytes.length)
        buf.appendBytes(bytes)
      }

      buf.getBytes
    }

    implicit val avetComp = new Ordering[Bytes] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(d0: Bytes, d1: Bytes): Int = {
        //comp.compare(x.toByteArray, y.toByteArray)

        /*val search = Any.parseFrom(d0).unpack(Datom)
        val x = Any.parseFrom(d1).unpack(Datom)

        var r: Int = 0

        if(search.a.isDefined){
          r = search.a.get.compareTo(x.a.get)
          if(r != 0) return r
        }

        if(search.v.isDefined){
          r = comp.compare(search.v.get.toByteArray, x.v.get.toByteArray)

          if(show.get() && search.a.isDefined){
            logger.info(s"${Console.RED_B}search ${search.a.get} v: ${x.v.get.asReadOnlyByteBuffer().getInt}${Console.RESET}")
          }

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

        r*/

        /*val b0 = Buffer.buffer(d0)
        val b1 = Buffer.buffer(d1)

        val j0 = new JsonArray(b0)
        val j1 = new JsonArray(b1)

        val aa0 = j0.getString(0)
        val av0 = j0.getValue(1)
        val ae0 = j0.getString(2)
        val at0 = j0.getLong(3)

        val ba0 = j0.getString(0)
        val bv0 = j0.getValue(1)
        val be0 = j0.getString(2)
        val bt0 = j0.getLong(3)

        println(j0, " ", j1)*/

        comp.compare(d0, d1)
      }
    }

    //implicit val vaetComp = DefaultComparators.ord

    val NUM_LEAF_ENTRIES = 64
    val NUM_META_ENTRIES = 64

    val indexId = "demo_db"

    implicit val global = ExecutionContext.global
    implicit val cache = new DefaultCache(100L * 1024L * 1024L, 10000)

    /*implicit val serializer = new GrpcByteSerializer(new Serializer[Datom] {
      override def serialize(t: Datom): Array[Byte] = t.toByteArray
      override def deserialize(b: Array[Byte]): Datom = Any.parseFrom(b).unpack(Datom)
    }, DefaultSerializers.byteSerializer)*/

   //implicit val storage = new CassandraStorage("indexes", NUM_LEAF_ENTRIES, NUM_META_ENTRIES, truncate = true)

    implicit val storage = new MemoryStorage(NUM_LEAF_ENTRIES, NUM_META_ENTRIES)
    implicit val ctx = new DefaultContext(indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new QueryableIndex()

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

    val result = Await.result(index.insert(datoms.map(_ -> EMPTY_ARRAY).map {  case (k, v) =>
      //Any.pack(k).toByteArray -> v
      datomToBytes(k) -> EMPTY_ARRAY
    }).flatMap(_ => ctx.save()), Duration.Inf)

    logger.info(s"result: ${result}")

    /*val it2 = index.findAll(Datom(a = Some("person/:name")))

    def findAll(): Future[Seq[Datom]] = {
      it2.hasNext().flatMap {
        case true => it2.next().flatMap { list =>
          findAll().map{list.map(_._1) ++ _}
        }
        case false => Future.successful(Seq.empty[Datom])
      }
    }*/

    /*val all = Await.result(index.inOrder(), Duration.Inf).map{case (k, _) => Any.parseFrom(k).unpack(Datom)}
      .map{d => d.a -> (if(d.a.get.compareTo("person/:age") == 0) d.v.get.asReadOnlyByteBuffer().getInt
        else new String(d.v.get.toByteArray)) -> d.e}*/

    val all = Await.result(index.inOrder(), Duration.Inf).map{  case (k, _) =>
      val arr = Buffer.buffer(k)

      /*val a = arr.getString(0)
      val v = arr.getBuffer(1)

      a -> (if(a.compareTo("person/:age") == 0) v.getInt(0) else new String(v.getBytes)) -> arr.getString(2)*/

      var len = arr.getInt(0)

      var pos = len + 4

      val a = arr.getString(4, pos)

      len = arr.getInt(pos)

      val v = arr.getBytes(pos + 4, pos + 4 + len)

      println(s"v pos: ${len}")

      a -> (if(a.compareTo("person/:age") == 0) ByteBuffer.allocate(4).put(v).flip().getInt() else new String(v))
    }

    logger.info(s"\nall: ${all}\n")

    /*val prefix = datomToBytes(Datom(a = Some("person/:name")))

    val it = index.findPrefix(prefix)(new Ordering[Bytes]{

      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(d0: Bytes, d1: Bytes): Int = {
        val s0 = d0.slice(0, d0.length - 1)
        val s1 = d1.slice(0, s0.length)

        val r = comp.compare(s0, s1)

        println(s"search: ${new String(s0)} x: ${new String(s1)} comp: ${r}")

        r
      }
    })

    def findAll(): Future[Seq[JsonArray]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(_._1).map{k => new JsonArray(Buffer.buffer(k))} ++ _}
        }
        case false => Future.successful(Seq.empty[JsonArray])
      }
    }

    val list = Await.result(findAll(), Duration.Inf).map { arr =>
      val a = arr.getString(0)
      val v = arr.getBuffer(1)

      a -> (if(a.compareTo("person/:age") == 0) v.getInt(0) else new String(v.getBytes)) -> arr.getString(2)
    }

    logger.info(s"\nall: ${list}\n")*/

    /*val it = index.findPrefix(Any.pack(prefix).toByteArray)(new Ordering[Bytes]{

      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(d0: Bytes, d1: Bytes): Int = {

        /*val search = Any.parseFrom(d0).unpack(Datom)
        val x = Any.parseFrom(d1).unpack(Datom)

        var r = search.a.get.compareTo(x.a.get)

        if(r != 0) return r

        -1*/

        val slice = d1.slice(0, d0.length)

        comp.compare(d0, slice)
      }
    })

    def findAll(): Future[Seq[Datom]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(_._1).map{k => Any.parseFrom(k).unpack(Datom)} ++ _}
        }
        case false => Future.successful(Seq.empty[Datom])
      }
    }

    val r = Await.result(findAll(), Duration.Inf).map { d =>
      d.a -> (if(d.a.get.compareTo("person/:age") == 0) d.v.get.asReadOnlyByteBuffer().getInt else new String(d.v.get.toByteArray)) -> d.e
    }

    println(r)*/
  }

}
