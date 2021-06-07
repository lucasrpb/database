package services.scalable.database

import ch.qos.logback.classic.{Level, LoggerContext}
import com.google.common.base.Charsets
import com.google.common.primitives.UnsignedBytes
import com.google.protobuf.ByteString
import com.google.protobuf.any.Any
import org.apache.commons.lang3.RandomStringUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.slf4j.LoggerFactory
import services.scalable.database.grpc._
import services.scalable.index._
import services.scalable.index.impl._

import java.nio.ByteBuffer
import java.util.concurrent.ThreadLocalRandom
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class DatomSpec extends AnyFlatSpec with Repeatable {

  override val times = 1000

  implicit def strToBytes(str: String): Bytes = str.getBytes(Charsets.UTF_8)

  val EMPTY_ARRAY = Array.empty[Byte]

  val rand = ThreadLocalRandom.current()

  //val logger = LoggerFactory.getLogger(this.getClass)
  val factory = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
  val logger = factory.getLogger(this.getClass)

  "it " should "serialize and order datoms (serialized as array of arrays of bytes) correctly " in {

    implicit val avetOrdering = new Ordering[Datom] {
      val comp = UnsignedBytes.lexicographicalComparator()

      override def compare(search: K, x: K): Int = {
        var r: Int = 0

        if(!search.a.isEmpty){
          r = search.getA.compareTo(x.getA)
          if(r != 0) return r
        }

        if(!search.v.isEmpty){
          r = comp.compare(search.getV.toByteArray, x.getV.toByteArray)
          if(r != 0) return r
        }

        if(!search.e.isEmpty){
          r = search.getE.compareTo(x.getE)
          if(r != 0) return r
        }

        if(!search.t.isEmpty){
          r = search.getT.compare(x.getT)
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

    val index = new QueryableIndex()

    val n = rand.nextInt(1, 1000)

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
        Datom(e = Some(id), a = Some("person/:color"), v = Some(ByteString.copyFrom(color)), t = Some(now)),
        Datom(e = Some(id), a = Some("person/:ab"), v = Some(ByteString.copyFrom(RandomStringUtils.randomAlphanumeric(6).getBytes(Charsets.UTF_8))))
      )
    }

    val result = Await.result(index.insert(datoms.map(_ -> EMPTY_ARRAY).map {  case (avet, _) =>
      avet -> EMPTY_ARRAY
    }).flatMap(_ => ctx.save()), Duration.Inf)

    logger.info(s"result: ${result}")

    val data = Await.result(index.inOrder(), Duration.Inf).map { case (d, _) =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
    }

    logger.info(s"${Console.GREEN_B}data: ${data}${Console.RESET}\n")

    val age =  ByteString.copyFrom(ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip())
    val minAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(20).flip())
    val maxAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(50).flip())

    val inclusive = rand.nextBoolean()
    val upperInclusive = rand.nextBoolean()

    //val it = index.gt(term = datomToBytes(Datom(a = "person/:age", v = age)), inclusive = inclusive)(personAgeOrd)

    val properties = Seq("person/:ab", "person/:age", "person/:name", "person/:color")

    val names = datoms.filter(_.a.get.compareTo("person/:name") == 0)
    val abs = datoms.filter(_.a.get.compareTo("person/:ab") == 0)

    val p = "person/:age"//properties(rand.nextInt(0, properties.length))
    val pupper = properties(rand.nextInt(0, properties.length))

    val v = p match {
      case p if p.compareTo("person/:ab") == 0 => abs(rand.nextInt(0, abs.length)).getV
      case p if p.compareTo("person/:age") == 0 => age
      case p if p.compareTo("person/:color") == 0 => ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes(Charsets.UTF_8))
      case p if p.compareTo("person/:name") == 0 => names(rand.nextInt(0, names.length)).getV
    }

    val vupper = pupper match {
      case p if p.compareTo("person/:ab") == 0 => abs(rand.nextInt(0, abs.length)).getV
      case p if p.compareTo("person/:age") == 0 => maxAge
      case p if p.compareTo("person/:color") == 0 => ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes(Charsets.UTF_8))
      case p if p.compareTo("person/:name") == 0 => names(rand.nextInt(0, names.length)).getV
    }

    val prefixPresent = rand.nextBoolean()
    val upperPrefixPresent = rand.nextBoolean()

    val term = if(prefixPresent || rand.nextBoolean()) Datom(a = Some(p), v = Some(v)) else Datom(a = Some(p))
    val prefix: Option[Datom] = if(prefixPresent) Some(Datom(a = Some(p))) else None

    val upperTerm = if(upperPrefixPresent || rand.nextBoolean()) Datom(a = Some(pupper), v = Some(vupper)) else Datom(a = Some(pupper))
    val upperPrefix: Option[Datom] = if(upperPrefixPresent) Some(Datom(a = Some(pupper))) else None

    /*val term = Datom(a = Some(p), v = Some(minAge))
    val upperTerm = Datom(a = Some(p), v = Some(maxAge))

    val prefix = Some(Datom(a = Some("person/:age")))
    val upperPrefix = prefix*/

    //val it = index.gt(term = term, prefix = prefix, inclusive = inclusive)

    /*val it = index.gt(term = term, prefix = prefix, inclusive = inclusive)*/
    /*val it = index.lt(term, prefix, inclusive)*/
    val it = index.interval(term, upperTerm, prefix, upperPrefix, inclusive, upperInclusive)

    //it.setLimit(5)

    def checkInterval(k: K): Boolean = {
      (prefix.isEmpty || avetOrdering.gteq(prefix.get, term)) &&
        (upperPrefix.isEmpty || avetOrdering.lteq(upperPrefix.get, upperTerm)) &&
        (inclusive && avetOrdering.gteq(k, term) || avetOrdering.gt(k, term)) &&
        (upperInclusive && avetOrdering.lteq(k, upperTerm) || avetOrdering.lt(k, upperTerm))
    }

    def findAll(): Future[Seq[Datom]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(_._1) ++ _}
        }
        case false => Future.successful(Seq.empty[Datom])
      }
    }

    val r = Await.result(findAll(), Duration.Inf)

    /*val shouldbe = datoms.filter(_.getA.compareTo(prefix.getA) == 0).filter{d => inclusive && avetOrdering.lteq(d, term) || avetOrdering.lt(d, term)}
      .sorted.map { d =>
        d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
      }*/

    val sorted = datoms.sorted.map { d =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
    }

    assert(sorted == data)

    val shouldbe = datoms.sorted.filter{d => checkInterval(d)}.map { d =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
      //d.getV.asReadOnlyByteBuffer().getInt()
    }

    val formatted =  r.map { d =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
      //d.getV.asReadOnlyByteBuffer().getInt()
    }

    logger.info(s"\n $p ${(if(p.compareTo("person/:age") == 0) v.asReadOnlyByteBuffer().getInt() else new String(v.toByteArray))} prefix: ${prefix} inclusive: ${inclusive} upperTerm: ${upperTerm} upperInclusive: ${upperInclusive} result: ${formatted}")
    logger.info(s"\n $p ${(if(p.compareTo("person/:age") == 0) v.asReadOnlyByteBuffer().getInt() else new String(v.toByteArray))} prefix: ${prefix} inclusive: ${inclusive} upperTerm: ${upperTerm} upperInclusive: ${upperInclusive} should be: ${shouldbe}")

    //index.prettyPrint()(d => (d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e).toString, buf => "")

    assert(formatted == shouldbe)

  }

}
