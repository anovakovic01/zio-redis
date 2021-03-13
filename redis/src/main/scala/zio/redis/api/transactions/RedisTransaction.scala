package zio.redis.api.transactions

import zio.ZIO
import zio.redis.Input.NoInput
import zio.redis.Output.{ StringOutput, UnitOutput }
import zio.redis.{ Output, RedisCommand, RedisError, RedisExecutor }

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
  def commit: ZIO[RedisExecutor, RedisError, Out] =
    for {
      _      <- Multi.run()
      _      <- run
      outs   <- ZIO.accessM[TransactionState](_.get.get)
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

  protected def run: ZIO[TransactionState with RedisExecutor, RedisError, Unit]
}

private[redis] object RedisTransaction {
  final case class Single[Out](command: RedisTrCommand[Out]) extends RedisTransaction[Out] {
    def run: ZIO[RedisExecutor, RedisError, Unit] =
      ZIO.accessM[TransactionState] { st =>
        for {
          _ <- command.run()
          _ <- st.get.getAndUpdate(_ + command.output)
        } yield ()
      }
  }

  final case class Zip[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[(A, B)] {
    def run: ZIO[TransactionState with RedisExecutor, RedisError, Unit] = (left.run <*> right.run).unit
  }

  final case class ZipLeft[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[A] {
    def run: ZIO[TransactionState with RedisExecutor, RedisError, Unit] = left.run <* right.run
  }

  final case class ZipRight[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[B] {
    def run: ZIO[TransactionState with RedisExecutor, RedisError, Unit] = left.run *> right.run
  }

  final val Multi: RedisCommand[Unit, String] = RedisCommand("MULTI", NoInput, StringOutput)

  final def exec(output: List[Output[Any]]): RedisCommand[Unit, List[Any]] =
    RedisCommand("EXEC", NoInput, ExecOutput(output))
}
