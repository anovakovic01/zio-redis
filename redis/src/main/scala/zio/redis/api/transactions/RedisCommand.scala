package zio.redis.api.transactions

import zio.redis.{ Input, Output }

case class RedisCommand[-In, +Out](name: String, input: Input[In], output: Output[Out])
