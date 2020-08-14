package zio.redis

import zio.{ Chunk, Task }
import zio.redis.Input._
import zio.test._
import zio.test.Assertion._
import BitFieldCommand._
import BitFieldType._
import BitOperation._
import zio.duration._

object InputSpec extends BaseSpec {
  def spec: ZSpec[environment.TestEnvironment, Any] =
    suite("Input encoders")(
      suite("AbsTtl")(
        testM("valid value") {
          for {
            result <- Task(AbsTtlInput.encode(AbsTtl))
          } yield assert(result)(equalTo(Chunk.single("$6\r\nABSTTL\r\n")))
        }
      ),
      suite("Aggregate")(
        testM("max") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Max))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nMAX\r\n")))
        },
        testM("min") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Min))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nMIN\r\n")))
        },
        testM("sum") {
          for {
            result <- Task(AggregateInput.encode(Aggregate.Sum))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nSUM\r\n")))
        }
      ),
      suite("Auth")(
        testM("with empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("")))
          } yield assert(result)(equalTo(Chunk("$4\r\nAUTH\r\n", "$0\r\n\r\n")))
        },
        testM("with non-empty password") {
          for {
            result <- Task(AuthInput.encode(Auth("pass")))
          } yield assert(result)(equalTo(Chunk("$4\r\nAUTH\r\n", "$4\r\npass\r\n")))
        }
      ),
      suite("Bool")(
        testM("true") {
          for {
            result <- Task(BoolInput.encode(true))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n1\r\n")))
        },
        testM("false") {
          for {
            result <- Task(BoolInput.encode(false))
          } yield assert(result)(equalTo(Chunk.single("$1\r\n0\r\n")))
        }
      ),
      suite("BitFieldCommand")(
        testM("get with unsigned type and positive offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 2)))
          } yield assert(result)(equalTo(Chunk("$3\r\nGET\r\n", "$2\r\nu3\r\n", "$1\r\n2\r\n")))
        },
        testM("get with signed type and negative offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(SignedInt(3), -2)))
          } yield assert(result)(equalTo(Chunk("$3\r\nGET\r\n", "$2\r\ni3\r\n", "$2\r\n-2\r\n")))
        },
        testM("get with unsigned type and zero offset") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldGet(UnsignedInt(3), 0)))
          } yield assert(result)(equalTo(Chunk("$3\r\nGET\r\n", "$2\r\nu3\r\n", "$1\r\n0\r\n")))
        },
        testM("set with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(Chunk("$3\r\nSET\r\n", "$2\r\nu3\r\n", "$1\r\n2\r\n", "$3\r\n100\r\n")))
        },
        testM("set with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(Chunk("$3\r\nSET\r\n", "$2\r\ni3\r\n", "$2\r\n-2\r\n", "$4\r\n-100\r\n")))
        },
        testM("set with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldSet(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(Chunk("$3\r\nSET\r\n", "$2\r\nu3\r\n", "$1\r\n0\r\n", "$1\r\n0\r\n")))
        },
        testM("incr with unsigned type, positive offset and positive value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 2, 100L)))
          } yield assert(result)(equalTo(Chunk("$6\r\nINCRBY\r\n", "$2\r\nu3\r\n", "$1\r\n2\r\n", "$3\r\n100\r\n")))
        },
        testM("incr with signed type, negative offset and negative value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(SignedInt(3), -2, -100L)))
          } yield assert(result)(equalTo(Chunk("$6\r\nINCRBY\r\n", "$2\r\ni3\r\n", "$2\r\n-2\r\n", "$4\r\n-100\r\n")))
        },
        testM("incr with unsigned type, zero offset and zero value") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldIncr(UnsignedInt(3), 0, 0L)))
          } yield assert(result)(equalTo(Chunk("$6\r\nINCRBY\r\n", "$2\r\nu3\r\n", "$1\r\n0\r\n", "$1\r\n0\r\n")))
        },
        testM("overflow sat") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Sat))
          } yield assert(result)(equalTo(Chunk("$8\r\nOVERFLOW\r\n", "$3\r\nSAT\r\n")))
        },
        testM("overflow fail") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Fail))
          } yield assert(result)(equalTo(Chunk("$8\r\nOVERFLOW\r\n", "$4\r\nFAIL\r\n")))
        },
        testM("overflow warp") {
          for {
            result <- Task(BitFieldCommandInput.encode(BitFieldOverflow.Wrap))
          } yield assert(result)(equalTo(Chunk("$8\r\nOVERFLOW\r\n", "$4\r\nWRAP\r\n")))
        }
      ),
      suite("BitOperation")(
        testM("and") {
          for {
            result <- Task(BitOperationInput.encode(AND))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nAND\r\n")))
        },
        testM("or") {
          for {
            result <- Task(BitOperationInput.encode(OR))
          } yield assert(result)(equalTo(Chunk.single("$2\r\nOR\r\n")))
        },
        testM("xor") {
          for {
            result <- Task(BitOperationInput.encode(XOR))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nXOR\r\n")))
        },
        testM("not") {
          for {
            result <- Task(BitOperationInput.encode(NOT))
          } yield assert(result)(equalTo(Chunk.single("$3\r\nNOT\r\n")))
        }
      ),
      suite("BitPosRange")(
        testM("with only start") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(1.second.toMillis, None)))
          } yield assert(result)(equalTo(Chunk.single("$4\r\n1000\r\n")))
        },
        testM("with start and the end") {
          for {
            result <- Task(BitPosRangeInput.encode(BitPosRange(0.second.toMillis, Some(1.second.toMillis))))
          } yield assert(result)(equalTo(Chunk("$1\r\n0\r\n", "$4\r\n1000\r\n")))
        }
      ),
      suite("Changed")(
        testM("valid value") {
          for {
            result <- Task(ChangedInput.encode(Changed))
          } yield assert(result)(equalTo(Chunk("$2\r\nCH\r\n")))
        }
      ),
      suite("Copy")(
        testM("valid value") {
          for {
            result <- Task(CopyInput.encode(Copy))
          } yield assert(result)(equalTo(Chunk("$4\r\nCOPY\r\n")))
        }
      )
    )
}