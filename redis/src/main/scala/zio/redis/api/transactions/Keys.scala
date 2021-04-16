package zio.redis.api.transactions

import zio.Chunk
import zio.duration._
import zio.redis.Input._
import zio.redis.Output._
import zio.redis.api.transactions.RedisTransaction.Single
import zio.redis.{ RedisCommand => _, _ }

import java.time.Instant
import scala.util.matching.Regex

trait Keys {
  import Keys.{ Keys => _, _ }

  /**
   * Removes the specified keys. A key is ignored if it does not exist.
   *
   * @param key one required key
   * @param keys maybe rest of the keys
   * @return The number of keys that were removed.
   *
   * @see [[unlink]]
   */
  final def del(key: String, keys: String*): RedisTransaction[Long] = Single((key, keys.toList), Del)

  /**
   * Serialize the value stored at key in a Redis-specific format and return it to the user.
   *
   * @param key key
   * @return bytes for value stored at key
   */
  final def dump(key: String): RedisTransaction[Chunk[Byte]] = Single(key, Dump)

  /**
   * The number of keys existing among the ones specified as arguments. Keys mentioned multiple times and existing are
   * counted multiple times.
   *
   * @param key one required key
   * @param keys maybe rest of the keys
   * @return The number of keys existing.
   */
  final def exists(key: String, keys: String*): RedisTransaction[Long] = Single((key, keys.toList), Exists)

  /**
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
   *
   * @param key key
   * @param timeout timeout
   * @return true, if the timeout was set, false if the key didn't exist
   *
   * @see [[expireAt]]
   */
  final def expire(key: String, timeout: Duration): RedisTransaction[Boolean] = Single((key, timeout), Expire)

  /**
   * Deletes the key at the specific timestamp. A timestamp in the past will delete the key immediately.
   *
   * @param key key
   * @param timestamp an absolute Unix timestamp (seconds since January 1, 1970)
   * @return true, if the timeout was set, false if the key didn't exist
   *
   * @see [[expire]]
   */
  final def expireAt(key: String, timestamp: Instant): RedisTransaction[Boolean] = Single((key, timestamp), ExpireAt)

  /**
   * Returns all keys matching pattern.
   *
   * @param pattern string pattern
   * @return keys matching pattern
   */
  final def keys(pattern: String): RedisTransaction[Chunk[String]] = Single(pattern, Keys.Keys)

  /**
   * Atomically transfer a key from a source Redis instance to a destination Redis instance. On success the key is deleted
   * from the original instance and is guaranteed to exist in the target instance.
   *
   * @param host remote redis host
   * @param port remote redis instance port
   * @param key key to be transferred or empty string if using the keys option
   * @param destinationDb remote database id
   * @param timeout specifies the longest period without blocking which is allowed during the transfer
   * @param auth optionally provide password for the remote instance
   * @param copy copy option, to not remove the key from the local instance
   * @param replace replace option, to replace existing key on the remote instance
   * @param keys keys option, to migrate multiple keys, non empty list of keys
   * @return string OK on success, or NOKEY if no keys were found in the source instance
   */
  final def migrate(
    host: String,
    port: Long,
    key: String,
    destinationDb: Long,
    timeout: Duration,
    auth: Option[Auth] = None,
    copy: Option[Copy] = None,
    replace: Option[Replace] = None,
    keys: Option[(String, List[String])]
  ): RedisTransaction[String] =
    Single((host, port, key, destinationDb, timeout.toMillis, copy, replace, auth, keys), Migrate)

  /**
   * Move key from the currently selected database to the specified destination database. When key already
   * exists in the destination database, or it does not exist in the source database, it does nothing.
   *
   * @param key key
   * @param destination_db destination database id
   * @return true if the key was moved
   */
  final def move(key: String, destination_db: Long): RedisTransaction[Boolean] =
    Single((key, destination_db), Move)

  /**
   * Remove the existing timeout on key
   *
   * @param key key
   * @return true if timeout was removed, false if key does not exist or does not have an associated timeout
   */
  final def persist(key: String): RedisTransaction[Boolean] = Single(key, Persist)

  /**
   * Set a timeout on key. After the timeout has expired, the key will automatically be deleted.
   *
   * @param key key
   * @param timeout timeout
   * @return true, if the timeout was set, false if the key didn't exist
   *
   * @see [[pExpireAt]]
   */
  final def pExpire(key: String, timeout: Duration): RedisTransaction[Boolean] =
    Single((key, timeout), PExpire)

  /**
   * Deletes the key at the specific timestamp. A timestamp in the past will delete the key immediately.
   *
   * @param key key
   * @param timestamp an absolute Unix timestamp (milliseconds since January 1, 1970)
   * @return true, if the timeout was set, false if the key didn't exist
   *
   * @see [[pExpire]]
   */
  final def pExpireAt(key: String, timestamp: Instant): RedisTransaction[Boolean] =
    Single((key, timestamp), PExpireAt)

  /**
   * Returns the remaining time to live of a key that has a timeout.
   *
   * @param key key
   * @return remaining time to live of a key that has a timeout, error otherwise
   */
  final def pTtl(key: String): RedisTransaction[Duration] = Single(key, PTtl)

  /**
   * Return a random key from the currently selected database.
   * @return key or None when the database is empty.
   */
  final def randomKey(): RedisTransaction[Option[String]] = Single((), RandomKey)

  /**
   * Renames key to newKey. It returns an error when key does not exist. If newKey already exists it is overwritten.
   *
   * @param key key to be renamed
   * @param newKey new name
   * @return unit if successful, error otherwise
   */
  final def rename(key: String, newKey: String): RedisTransaction[Unit] = Single((key, newKey), Rename)

  /**
   * Renames key to newKey if newKey does not yet exist. It returns an error when key does not exist.
   *
   * @param key key to be renamed
   * @param newKey new name
   * @return true if key was renamed to newKey, false if newKey already exists
   */
  final def renameNx(key: String, newKey: String): RedisTransaction[Boolean] = Single((key, newKey), RenameNx)

  /**
   * Create a key associated with a value that is obtained by deserializing the provided serialized value. Error when key
   * already exists unless you use the REPLACE option.
   *
   * @param key key
   * @param ttl time to live in milliseconds, 0 if without any expire
   * @param value serialized value
   * @param replace replace option, replace if existing
   * @param absTtl absolute ttl option, ttl should represent an absolute Unix timestamp (in milliseconds) in which the key will expire.
   * @param idleTime idle time based eviction policy
   * @param freq frequency based eviction policy
   * @return unit on success
   */
  final def restore(
    key: String,
    ttl: Long,
    value: Chunk[Byte],
    replace: Option[Replace] = None,
    absTtl: Option[AbsTtl] = None,
    idleTime: Option[IdleTime] = None,
    freq: Option[Freq] = None
  ): RedisTransaction[Unit] = Single((key, ttl, value, replace, absTtl, idleTime, freq), Restore)

  /**
   * Iterates the set of keys in the currently selected Redis database. An iteration starts when the cursor is set to 0,
   * and terminates when the cursor returned by the server is 0.
   *
   * @param cursor cursor value, starts with zero
   * @param pattern key pattern
   * @param count count option, specifies number of returned elements per call
   * @param `type` type option, filter to only return objects that match a given type
   * @return returns an updated cursor that the user needs to use as the cursor argument in the next call along with the values
   */
  final def scan(
    cursor: Long,
    pattern: Option[Regex] = None,
    count: Option[Long] = None,
    `type`: Option[String] = None
  ): RedisTransaction[(Long, Chunk[String])] = Single((cursor, pattern, count, `type`), Scan)

  /**
   * Alters the last access time of a key(s). A key is ignored if it does not exist.
   *
   * @param key one required key
   * @param keys maybe rest of the keys
   * @return The number of keys that were touched.
   */
  final def touch(key: String, keys: String*): RedisTransaction[Long] = Single((key, keys.toList), Touch)

  /**
   * Returns the remaining time to live of a key that has a timeout.
   *
   * @param key key
   * @return remaining time to live of a key that has a timeout, error otherwise
   */
  final def ttl(key: String): RedisTransaction[Duration] = Single(key, Ttl)

  /**
   * Returns the string representation of the type of the value stored at key.
   *
   * @param key key
   * @return type of the value stored at key
   */
  final def typeOf(key: String): RedisTransaction[RedisType] = Single(key, TypeOf)

  /**
   * Removes the specified keys. A key is ignored if it does not exist. The command performs the actual memory reclaiming
   * in a different thread, so it is not blocking.
   *
   * @param key one required key
   * @param keys maybe rest of the keys
   * @return The number of keys that were unlinked.
   *
   * @see [[del]]
   */
  final def unlink(key: String, keys: String*): RedisTransaction[Long] = Single((key, keys.toList), Unlink)

  /**
   * This command blocks the current client until all the previous write commands are successfully transferred and acknowledged
   * by at least the specified number of replicas.
   *
   * @param replicas minimum replicas to reach
   * @param timeout specified as a Duration, 0 means to block forever
   * @return the number of replicas reached both in case of failure and success
   */
  final def wait_(replicas: Long, timeout: Duration): RedisTransaction[Long] =
    Single((replicas, timeout.toMillis), Wait)
}

private[redis] object Keys {
  final val Del: RedisCommand[(String, List[String]), Long] =
    RedisCommand("DEL", NonEmptyList(StringInput), LongOutput)

  final val Dump: RedisCommand[String, Chunk[Byte]] = RedisCommand("DUMP", StringInput, BulkStringOutput)

  final val Exists: RedisCommand[(String, List[String]), Long] =
    RedisCommand("EXISTS", NonEmptyList(StringInput), LongOutput)

  final val Expire: RedisCommand[(String, Duration), Boolean] =
    RedisCommand("EXPIRE", Tuple2(StringInput, DurationSecondsInput), BoolOutput)

  final val ExpireAt: RedisCommand[(String, Instant), Boolean] =
    RedisCommand("EXPIREAT", Tuple2(StringInput, TimeSecondsInput), BoolOutput)

  final val Keys: RedisCommand[String, Chunk[String]] = RedisCommand("KEYS", StringInput, ChunkOutput)

  final val Migrate: RedisCommand[
    (String, Long, String, Long, Long, Option[Copy], Option[Replace], Option[Auth], Option[(String, List[String])]),
    String
  ] =
    RedisCommand(
      "MIGRATE",
      Tuple9(
        StringInput,
        LongInput,
        StringInput,
        LongInput,
        LongInput,
        OptionalInput(CopyInput),
        OptionalInput(ReplaceInput),
        OptionalInput(AuthInput),
        OptionalInput(NonEmptyList(StringInput))
      ),
      StringOutput
    )

  final val Move: RedisCommand[(String, Long), Boolean] =
    RedisCommand("MOVE", Tuple2(StringInput, LongInput), BoolOutput)

  final val Persist: RedisCommand[String, Boolean] = RedisCommand("PERSIST", StringInput, BoolOutput)

  final val PExpire: RedisCommand[(String, Duration), Boolean] =
    RedisCommand("PEXPIRE", Tuple2(StringInput, DurationMillisecondsInput), BoolOutput)

  final val PExpireAt: RedisCommand[(String, Instant), Boolean] =
    RedisCommand("PEXPIREAT", Tuple2(StringInput, TimeMillisecondsInput), BoolOutput)

  final val PTtl: RedisCommand[String, Duration] = RedisCommand("PTTL", StringInput, DurationMillisecondsOutput)

  final val RandomKey: RedisCommand[Unit, Option[String]] =
    RedisCommand("RANDOMKEY", NoInput, OptionalOutput(MultiStringOutput))

  final val Rename: RedisCommand[(String, String), Unit] =
    RedisCommand("RENAME", Tuple2(StringInput, StringInput), UnitOutput)

  final val RenameNx: RedisCommand[(String, String), Boolean] =
    RedisCommand("RENAMENX", Tuple2(StringInput, StringInput), BoolOutput)

  final val Restore
    : RedisCommand[(String, Long, Chunk[Byte], Option[Replace], Option[AbsTtl], Option[IdleTime], Option[Freq]), Unit] =
    RedisCommand(
      "RESTORE",
      Tuple7(
        StringInput,
        LongInput,
        ByteInput,
        OptionalInput(ReplaceInput),
        OptionalInput(AbsTtlInput),
        OptionalInput(IdleTimeInput),
        OptionalInput(FreqInput)
      ),
      UnitOutput
    )

  final val Scan: RedisCommand[(Long, Option[Regex], Option[Long], Option[String]), (Long, Chunk[String])] =
    RedisCommand(
      "SCAN",
      Tuple4(LongInput, OptionalInput(RegexInput), OptionalInput(LongInput), OptionalInput(StringInput)),
      ScanOutput
    )

  final val Touch: RedisCommand[(String, List[String]), Long] =
    RedisCommand("TOUCH", NonEmptyList(StringInput), LongOutput)

  final val Ttl: RedisCommand[String, Duration] = RedisCommand("TTL", StringInput, DurationSecondsOutput)

  final val TypeOf: RedisCommand[String, RedisType] = RedisCommand("TYPE", StringInput, TypeOutput)

  final val Unlink: RedisCommand[(String, List[String]), Long] =
    RedisCommand("UNLINK", NonEmptyList(StringInput), LongOutput)

  final val Wait: RedisCommand[(Long, Long), Long] = RedisCommand("WAIT", Tuple2(LongInput, LongInput), LongOutput)
}
