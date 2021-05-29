package services.scalable.database

import services.scalable.index.{Context, Index}
import scala.concurrent.ExecutionContext

class QueryableIndex[K, V](override implicit val ec: ExecutionContext, override val ctx: Context[K, V]) extends Index[K, V]()(ec, ctx) {



}
