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

    val comp = UnsignedBytes.lexicographicalComparator()

    val ord = new Ordering[Datom] {

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

    val termOrd = new Ordering[Datom]{
      override def compare(search: K, x: K): Int = {
        var r: Int = 0

        if(!search.a.isEmpty){
          r = search.getA.compareTo(x.getA)
          if(r != 0) return r
        }

        comp.compare(search.getV.toByteArray, x.getV.toByteArray)
      }
    }

    val NUM_LEAF_ENTRIES = 8
    val NUM_META_ENTRIES = 8

    val indexId = "demo_db"

    type K = Datom
    type V = Bytes

    implicit val cache = new DefaultCache[K, V](100L * 1024L * 1024L, 10000)

    implicit val storage = new MemoryStorage[K, V](NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, ord, cache)
    implicit val ctx = new DefaultContext[K, V](indexId, None, NUM_LEAF_ENTRIES, NUM_META_ENTRIES)(global, storage, cache, ord)

    logger.debug(s"${Await.result(storage.loadOrCreate(indexId), Duration.Inf)}")

    val index = new QueryableIndex()(global, ctx, ord)

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
    })(ord).flatMap(_ => ctx.save()), Duration.Inf)

    logger.info(s"result: ${result}")

    val iter = index.inOrder()(ord)

    val data = Await.result(TestHelper.all(iter), Duration.Inf).map { case (d, _) =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
    }

    logger.info(s"${Console.GREEN_B}data: ${data}${Console.RESET}\n")

    val age =  ByteString.copyFrom(ByteBuffer.allocate(4).putInt(rand.nextInt(18, 100)).flip())
    val minAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(rand.nextInt(20, 100)).flip())
    val maxAge = ByteString.copyFrom(ByteBuffer.allocate(4).putInt(rand.nextInt(30, 100)).flip())

    val inclusive = rand.nextBoolean()
    val upperInclusive = rand.nextBoolean()

    //val it = index.gt(term = datomToBytes(Datom(a = "person/:age", v = age)), inclusive = inclusive)(personAgeOrd)

    val properties = Seq("person/:ab", "person/:age", "person/:name", "person/:color")

    val names = datoms.filter(_.a.get.compareTo("person/:name") == 0)
    val abs = datoms.filter(_.a.get.compareTo("person/:ab") == 0)

    val p = properties(rand.nextInt(0, properties.length))
    val pgreater = properties.filter{_.compareTo(p) >= 0}
    val pupper = pgreater(rand.nextInt(0, pgreater.length))//properties(rand.nextInt(0, properties.length))

    var v = p match {
      case p if p.compareTo("person/:ab") == 0 => abs(rand.nextInt(0, abs.length)).getV
      case p if p.compareTo("person/:age") == 0 => minAge
      case p if p.compareTo("person/:color") == 0 => ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes(Charsets.UTF_8))
      case p if p.compareTo("person/:name") == 0 => names(rand.nextInt(0, names.length)).getV
    }

    var vupper = pupper match {
      case p if p.compareTo("person/:ab") == 0 => abs(rand.nextInt(0, abs.length)).getV
      case p if p.compareTo("person/:age") == 0 => maxAge
      case p if p.compareTo("person/:color") == 0 => ByteString.copyFrom(colors(rand.nextInt(0, colors.length)).getBytes(Charsets.UTF_8))
      case p if p.compareTo("person/:name") == 0 => names(rand.nextInt(0, names.length)).getV
    }

    if(comp.compare(vupper.toByteArray, v.toByteArray) < 0){
      val aux = vupper
      vupper = v
      v = aux
    }

    val prefixPresent = rand.nextBoolean()

    /*val term = if(prefixPresent) Datom(a = Some(p), v = Some(v)) else Datom(a = Some(p))
    val prefix: Option[Datom] = if(prefixPresent) Some(Datom(a = Some(p))) else None

    val upperTerm = if(upperPrefixPresent) Datom(a = Some(pupper), v = Some(vupper)) else Datom(a = Some(pupper))
    val upperPrefix: Option[Datom] = if(upperPrefixPresent) Some(Datom(a = Some(pupper))) else None*/

    val term = Datom(a = Some(p), v = Some(v))
    val prefix = if(prefixPresent) Some(Datom(a = Some(p))) else None

    val upperTerm = Datom(a = Some(pupper), v = Some(vupper))
    val upperPrefix = if(prefixPresent) Some(Datom(a = Some(pupper))) else None

    val prefixOrd = new Ordering[Datom] {
      override def compare(pre: K, k: K): Int = {
        if(prefixPresent) comp.compare(prefix.get.getA.getBytes(Charsets.UTF_8), k.getA.getBytes(Charsets.UTF_8)) else -1
      }
    }

    val upperPrefixOrd = new Ordering[Datom] {
      override def compare(pre: K, k: K): Int = {
        if(prefixPresent) comp.compare(upperPrefix.get.getA.getBytes(Charsets.UTF_8), k.getA.getBytes(Charsets.UTF_8)) else -1
      }
    }

    def checkLt(k: K): Boolean = {
      prefixOrd.equiv(k, k) && (inclusive && termOrd.lteq(k, term) || termOrd.lt(k, term))
    }

    def checkGt(k: K): Boolean = {
      prefixOrd.equiv(k, k) && (inclusive && termOrd.gteq(k, term) || termOrd.gt(k, term))
    }

    def checkInterval(k: K): Boolean = {
      (prefixOrd.equiv(k, k)) && (inclusive && termOrd.gteq(k, term) || termOrd.gt(k, term)) &&
        (upperPrefixOrd.equiv(k, k) && (upperInclusive && termOrd.lteq(k, upperTerm) || termOrd.lt(k, upperTerm)))
    }

    var it: RichAsyncIterator[Datom, Bytes] = null
    var shouldbe = Seq.empty[Datom]

    def findAll(): Future[Seq[Datom]] = {
      it.hasNext().flatMap {
        case true => it.next().flatMap { list =>
          findAll().map{list.map(_._1) ++ _}
        }
        case false => Future.successful(Seq.empty[Datom])
      }
    }

    var op = ""
    val reverse = rand.nextBoolean()

    rand.nextInt(1, 4) match {
      case 1 =>

        op = if(inclusive) "<=" else "<"
        it = index.lt(term = term, inclusive = inclusive, reverse)(prefixOrd, termOrd)
        shouldbe = datoms.sorted(ord).filter{d => checkLt(d)}

      case 2 =>

        op = if(inclusive) ">=" else ">"
        it = index.gt(term = term, inclusive = inclusive, reverse)(prefixOrd, termOrd)
        shouldbe = datoms.sorted(ord).filter{d => checkGt(d)}

      case 3 =>

        op = s"${if(inclusive) ">=" else ">"}, ${if(upperInclusive) "<=" else "<"}"
        it = index.interval(term, upperTerm, inclusive, upperInclusive, reverse)(prefixOrd, upperPrefixOrd, termOrd)
        shouldbe = datoms.sorted(ord).filter{d => checkInterval(d)}
    }

    //it.setLimit(5)

    /*op = if(inclusive) "<=" else "<" + " reverse"
    it = index.ltr(term = term, inclusive = inclusive)(prefixOrd, termOrd)

    shouldbe = datoms.sorted(ord).reverse.filter{d => checkLt(d)}*/

    /*op = s"${if(inclusive) ">=" else ">"}, ${if(upperInclusive) "<=" else "<"}"
    it = index.intervalr(term, upperTerm, inclusive, upperInclusive)(prefixOrd, upperPrefixOrd, termOrd)

    shouldbe = datoms.sorted(ord).reverse.filter{d => checkInterval(d)}*/

    /*op = if(inclusive) ">=" else ">"
    it = index.lt(term = term, inclusive = inclusive, reverse)(prefixOrd, termOrd)
    shouldbe = datoms.sorted(ord).filter{d => checkLt(d)}*/

    if(reverse) shouldbe = shouldbe.reverse

    /*op = if(inclusive) "<=" else "<"
    it = index.lt(term = term, inclusive = inclusive)(prefixOrd, termOrd)
    shouldbe = datoms.sorted(ord).filter{d => checkLt(d)}*/

    /*op = s"${if(inclusive) ">=" else ">"}, ${if(upperInclusive) "<=" else "<"}"
    it = index.interval(term, upperTerm, inclusive, upperInclusive)(prefixOrd, upperPrefixOrd, termOrd)
    shouldbe = datoms.sorted(ord).filter{d => checkInterval(d)}*/

    val r = Await.result(findAll(), Duration.Inf).map { d =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
      //d.getV.asReadOnlyByteBuffer().getInt()
    }

    val ref = shouldbe.map { d =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
      //d.getV.asReadOnlyByteBuffer().getInt()
    }

    /*val shouldbe = datoms.filter(_.getA.compareTo(prefix.getA) == 0).filter{d => inclusive && termOrdering.lteq(d, term) || termOrdering.lt(d, term)}
      .sorted.map { d =>
        d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
      }*/

    val sorted = datoms.sorted(ord).map { d =>
      d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e
    }

    assert(sorted == data)

    def formatDatom(d: Datom): String = {
      var arr = Seq.empty[String]

      if(d.a.isDefined){
        arr = arr :+ d.getA
      }

      if(d.v.isDefined){
        arr = arr :+ (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt().toString else new String(d.getV.toByteArray))
      }

      arr.mkString(",")
    }

    def formatDatomOpt(d: Option[Datom]): String = {
      if(d.isEmpty) return "[empty]"
      formatDatom(d.get)
    }

    logger.info(s"\n op: ${op} prefix ${formatDatomOpt(prefix)} lowerterm ${formatDatom(term)} inclusive: ${inclusive} upperTerm: ${formatDatom(upperTerm)} upperInclusive: ${upperInclusive} result: ${r}\n")
    logger.info(s"\n op: ${op} prefix ${formatDatomOpt(prefix)} lowerterm ${formatDatom(term)} inclusive: ${inclusive} upperTerm: ${formatDatom(upperTerm)} upperInclusive: ${upperInclusive} result: ${ref}")

    //index.prettyPrint()(d => (d.a -> (if(d.getA.compareTo("person/:age") == 0) d.getV.asReadOnlyByteBuffer().getInt() else new String(d.getV.toByteArray)) -> d.e).toString, buf => "")

    assert(r == ref)

  }

}
