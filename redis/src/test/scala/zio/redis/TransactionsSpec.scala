package zio.redis

import zio.ZLayer
import zio.clock.Clock
import zio.logging.Logging
import zio.redis.api.transactions.HList.{ HCons, HNil }
import zio.test.Assertion._
import zio.test._
import zio.duration._

object TransactionsSpec extends BaseSpec with api.transactions.Keys {
  val spec =
    suite("transactions")(
      testM("zip") {
        for {
          key    <- uuid
          value  <- uuid
          _      <- set(key, value)
          result <- (pExpire(key, 0.seconds) <*> exists("test")).commit
        } yield assert(result)(equalTo(HCons(true, HCons(0, HNil))))
      },
      testM("zip left") {
        for {
          key    <- uuid
          value  <- uuid
          _      <- set(key, value)
          result <- (pExpire(key, 1.second) <* exists(key)).commit
        } yield assert(result)(equalTo(HCons(true, HNil)))
      },
      testM("zip right") {
        for {
          key    <- uuid
          value  <- uuid
          _      <- set(key, value)
          result <- (pExpire(key, 100.seconds) *> exists(key)).commit
        } yield assert(result)(equalTo(HCons(1, HNil)))
      }
    ).provideCustomLayer((Logging.ignore ++ ZLayer.succeed(codec) >>> RedisExecutor.local.orDie) ++ Clock.live)
}
