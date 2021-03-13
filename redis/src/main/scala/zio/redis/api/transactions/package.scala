package zio.redis.api

import zio.{ Has, Ref }
import zio.redis.Output

package object transactions {
  type TransactionState = Has[Ref[List[Output[Any]]]] with Has[Ref[List[Long]]]
}
