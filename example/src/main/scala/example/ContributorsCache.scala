package example

import example.ApiError._
import example.Contributor._
import io.circe.parser.decode
import io.circe.syntax._
import sttp.client.asynchttpclient.zio.SttpClient
import sttp.client.circe.asJson
import sttp.client.{ UriContext, basicRequest }
import sttp.model.Uri

import zio._
import zio.redis._

object ContributorsCache {
  trait Service {
    def fetchAll(repository: Repository): IO[ApiError, Contributors]
  }

  lazy val live: ZLayer[RedisExecutor with SttpClient, Nothing, ContributorsCache] =
    ZLayer.fromFunction { env =>
      new Service {
        def fetchAll(repository: Repository): IO[ApiError, Contributors] =
          (read(repository) <> retrieve(repository)).provide(env)
      }
    }

  private[this] def read(repository: Repository): ZIO[RedisExecutor, ApiError, Contributors] =
    sMembers(repository.key).flatMap { c: Chunk[String] =>
      println(c)
      if (c.isEmpty)
        ZIO.fail(ApiError.CacheMiss)
      else
        ZIO.collectAll(c.map { value =>
          ZIO.fromEither(decode[Contributor](value)).catchAll(_ => ZIO.fail(CorruptedData))
        })
    }.map(Contributors(_)).catchAll { t =>
      println(t)
      ZIO.fail(ApiError.CacheMiss)
    }
//      .rightOrFail(ApiError.CorruptedData)

  private[this] def retrieve(repository: Repository): ZIO[RedisExecutor with SttpClient, ApiError, Contributors] =
    for {
      req          <- ZIO.succeed(basicRequest.get(urlOf(repository)).response(asJson[Chunk[Contributor]]))
      res          <- SttpClient.send(req).orElseFail(GithubUnreachable)
      contributors <- res.body.fold(_ => ZIO.fail(UnknownProject), ZIO.succeed(_))
      _            <- cache(repository, contributors)
    } yield Contributors(contributors)

  private def cache(repository: Repository, contributors: Chunk[Contributor]): URIO[RedisExecutor, Any] =
    ZIO
      .fromOption(NonEmptyChunk.fromChunk(contributors))
      .map(_.map(_.asJson.noSpaces))
      .flatMap(data => ZIO.collectAll(data.map(sAdd(repository.key, _))))
      .catchAll { t =>
        println(t)
        ZIO.fail(t)
      }
      .ignore

  private[this] def urlOf(repository: Repository): Uri =
    uri"https://api.github.com/repos/${repository.owner}/${repository.name}/contributors"
}
