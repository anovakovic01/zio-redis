package zio.redis.api.transactions

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.api.transactions.HList._
import zio.redis.{ Input, Output, RedisError, RedisExecutor }

sealed trait RedisTransaction[+Out] {
  import RedisTransaction._

  // QUEUED

  // SADD key value
  // import zio.redis._
  // import zio.redis.{ transactions => tr }

  // (tr.sAdd(key, value) <*>
  //  tr.sAdd(key, value) *>
  //  cmd3).commit

  // How to handle exec output?
  def commit: ZIO[RedisExecutor, RedisError, Out] = ???
  for {
    _      <- Multi.run()
    outs   <- send
    result <- exec(outs).run()
  } yield result

  // Should I overload these combinators in order to transform RedisCommands.
  def zip[A](right: RedisTransaction[A]): RedisTransaction[(Out, A)] =
    Zip(this, right)

  def zipLeft[A](right: RedisTransaction[A]): RedisTransaction[Out] =
    ZipLeft(this, right)

  def zipRight[A](right: RedisTransaction[A]): RedisTransaction[A] =
    ZipRight(this, right)

  def <*>[A](right: RedisTransaction[A]): RedisTransaction[(Out, A)] =
    zip(right)

  def <*[A](right: RedisTransaction[A]): RedisTransaction[Out] =
    zipLeft(right)

  def *>[A](right: RedisTransaction[A]): RedisTransaction[A] =
    zipRight(right)

  def send: ZIO[RedisExecutor, RedisError, HList]
}

private[redis] object RedisTransaction {
  final case class Single[In, +Out](in: In, command: RedisCommand[In, Out]) extends RedisTransaction[Out] {
    def send: ZIO[RedisExecutor, RedisError, HList] =
      ZIO
        .accessM[RedisExecutor](_.get.execute(Input.StringInput.encode(command.name) ++ command.input.encode(in)))
        .as(HCons(command.output, HNil))
        .refineToOrDie[RedisError]
  }

  final case class Zip[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[(A, B)] {
    def send: ZIO[RedisExecutor, RedisError, HList] =
      (left.send <*> right.send).map { case (lout, rout) =>
        lout.concat(rout)
      }
  }

  final case class ZipLeft[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[A] {
    def send: ZIO[RedisExecutor, RedisError, HList] =
      left.send <* right.send
  }

  final case class ZipRight[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[B] {
    def send: ZIO[RedisExecutor, RedisError, HList] =
      left.send *> right.send
  }

  final val Multi: zio.redis.RedisCommand[Unit, String] =
    zio.redis.RedisCommand("MULTI", NoInput, StringOutput)

  final def exec(output: HList): zio.redis.RedisCommand[Unit, HList] =
    zio.redis.RedisCommand("EXEC", NoInput, ExecOutput(output))
}
