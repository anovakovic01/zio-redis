package zio.redis.api.transactions

import zio.{ Chunk, ZIO }
import zio.redis.Input._
import zio.redis.Output._
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
  def commit: ZIO[RedisExecutor, RedisError, HList] =
    for {
      _      <- Multi.run(())
      outs   <- send
      result <- exec(outs).run(())
    } yield result

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

  def send: ZIO[RedisExecutor, RedisError, Chunk[Output[_]]]
}

private[redis] object RedisTransaction {
  final case class Single[In, +Out](in: In, command: RedisCommand[In, Out]) extends RedisTransaction[Out] {
    def send: ZIO[RedisExecutor, RedisError, Chunk[Output[_]]] =
      ZIO
        .accessM[RedisExecutor] { exec =>
          val svc   = exec.get
          val codec = svc.codec
          val cmd   = Varargs(StringInput).encode(command.name.split(" "))(codec) ++ command.input.encode(in)(codec)
          svc
            .execute(cmd)
            .flatMap[Any, Throwable, Unit](out => ZIO.effect(QueuedOutput.unsafeDecode(out)(codec)))
            .as(Chunk.single(command.output))
        }
        .refineToOrDie[RedisError]
  }

  final case class Zip[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[(A, B)] {
    def send: ZIO[RedisExecutor, RedisError, Chunk[Output[_]]] =
      (left.send <*> right.send).map { case (lout, rout) =>
        lout.concat(rout)
      }
  }

  final case class ZipLeft[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[A] {
    def send: ZIO[RedisExecutor, RedisError, Chunk[Output[_]]] =
      (left.send <*> right.send).map { case (lout, rout) =>
        lout.concat(rout.map(_ => IgnoreOutput))
      }
  }

  final case class ZipRight[A, B](left: RedisTransaction[A], right: RedisTransaction[B]) extends RedisTransaction[B] {
    def send: ZIO[RedisExecutor, RedisError, Chunk[Output[_]]] =
      (left.send <*> right.send).map { case (lout, rout) =>
        lout.map(_ => IgnoreOutput).concat(rout)
      }
  }

  final val Multi: RedisCommand[Unit, Boolean] =
    RedisCommand("MULTI", NoInput, SetOutput)

  final def exec(outputs: Chunk[Output[_]]): RedisCommand[Unit, HList] =
    RedisCommand("EXEC", NoInput, ExecOutput(outputs))
}
