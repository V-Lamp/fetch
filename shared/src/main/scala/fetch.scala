/*
 * Copyright 2016-2018 47 Degrees, LLC. <http://www.47deg.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fetch

import scala.collection.immutable.Map
import scala.util.control.NoStackTrace

import scala.concurrent.duration.MILLISECONDS

import cats._
//import cats.instances.list._
import cats.data._
import cats.implicits._
import cats.effect._
import cats.effect.concurrent.{Ref, Deferred}
import cats.temp.par._
//import cats.syntax.all._
//import cats.data.NonEmptyList


object `package` {
  // Fetch queries
  sealed trait FetchRequest[F[_]] extends Product with Serializable

  // A query to a remote data source
  sealed trait FetchQuery[F[_], I, A] extends FetchRequest[F] {
    def dataSource: DataSource[I, A]
    def identities: NonEmptyList[I]
  }
  case class FetchOne[F[_], I, A](id: I, ds: DataSource[I, A]) extends FetchQuery[F, I, A] {
    override def identities: NonEmptyList[I] = NonEmptyList(id, List.empty[I])
    override def dataSource: DataSource[I, A] = ds
  }
  case class Batch[F[_], I, A](ids: NonEmptyList[I], ds: DataSource[I, A]) extends FetchQuery[F, I, A] {
    override def identities: NonEmptyList[I] = ids
    override def dataSource: DataSource[I, A] = ds
  }

  // Fetch result states
  sealed trait FetchStatus
  case class FetchDone[A](result: A) extends FetchStatus
  case class FetchMissing() extends FetchStatus

  // Fetch errors
  sealed trait FetchException[F[_]] extends Throwable with NoStackTrace {
    def environment: Env[F]
  }
  case class MissingIdentity[F[_], I, A](i: I, request: FetchQuery[F, I, A], environment: Env[F]) extends FetchException[F]
  case class UnhandledException[F[_]](e: Throwable, environment: Env[F]) extends FetchException[F]

  // In-progress request
  case class BlockedRequest[F[_]](request: FetchRequest[F], result: FetchStatus => F[Unit])

  /* Combines the identities of two `FetchQuery` to the same data source. */
  private def combineIdentities[F[_], I, A](x: FetchQuery[F, I, A], y: FetchQuery[F, I, A]): NonEmptyList[I] = {
    y.identities.foldLeft(x.identities) {
      case (acc, i) => if (acc.exists(_ == i)) acc else NonEmptyList(acc.head, acc.tail :+ i)
    }
  }

  /* Combines two requests to the same data source. */
  private def combineRequests[F[_]](x: BlockedRequest[F], y: BlockedRequest[F]): BlockedRequest[F] = (x.request, y.request) match {
    case (a@FetchOne(aId, ds), b@FetchOne(anotherId, _)) =>
      if (aId == anotherId)  {
        val newRequest = FetchOne(aId, ds)
        val newResult = (r: FetchStatus) => (x.result(r), y.result(r)).tupled >> IO.unit
        BlockedRequest(newRequest, newResult)
      } else {
        val newRequest = Batch(combineIdentities(a, b), ds)
        val newResult = (r: FetchStatus) => r match {
          case FetchDone(m : Map[Any, Any]) => {
            val xResult = m.get(aId).map(FetchDone(_)).getOrElse(FetchMissing())
            val yResult = m.get(anotherId).map(FetchDone(_)).getOrElse(FetchMissing())
              (x.result(xResult), y.result(yResult)).tupled >> IO.unit
          }

          case FetchMissing() =>
            (x.result(r), y.result(r)).tupled >> IO.unit
        }
        BlockedRequest(newRequest, newResult)
      }

    case (a@FetchOne(oneId, ds), b@Batch(anotherIds, _)) =>
      val newRequest = Batch(combineIdentities(a, b), ds)
      val newResult = (r: FetchStatus) => r match {
        case FetchDone(m : Map[Any, Any]) => {
          val oneResult = m.get(oneId).map(FetchDone(_)).getOrElse(FetchMissing())

          (x.result(oneResult), y.result(r)).tupled >> IO.unit
        }

        case FetchMissing() =>
          (x.result(r), y.result(r)).tupled >> IO.unit
      }
      BlockedRequest(newRequest, newResult)

    case (a@Batch(manyId, ds), b@FetchOne(oneId, _)) =>
      val newRequest = Batch(combineIdentities(a, b), ds)
      val newResult = (r: FetchStatus) => r match {
        case FetchDone(m : Map[Any, Any]) => {
          val oneResult = m.get(oneId).map(FetchDone(_)).getOrElse(FetchMissing())
            (x.result(r), y.result(oneResult)).tupled >> IO.unit
        }

        case FetchMissing() =>
          (x.result(r), y.result(r)).tupled >> IO.unit
      }
      BlockedRequest(newRequest, newResult)

    case (a@Batch(manyId, ds), b@Batch(otherId, _)) =>
      val newRequest = Batch(combineIdentities(a, b), ds)
      val newResult = (r: FetchStatus) => (x.result(r), y.result(r)).tupled >> IO.unit
      BlockedRequest(newRequest, newResult)
  }

  /* A map from datasources to blocked requests used to group requests to the same data source. */
  case class RequestMap[F[_]](m: Map[DataSource[F, Any, Any], BlockedRequest[F]])

  /* Combine two `RequestMap` instances to batch requests to the same data source. */
  private def combineRequestMaps[F[_]](x: RequestMap[F], y: RequestMap[F]): RequestMap[F] =
    RequestMap(
      x.m.foldLeft(y.m) {
        case (acc, (ds, blocked)) => {
          val combinedReq: BlockedRequest = acc.get(ds).fold(blocked)(combineRequests(blocked, _))
          acc.updated(ds, combinedReq)
        }
      }
    )

  // `Fetch` result data type
  sealed trait FetchResult[F[_], A]
  case class Done[F[_], A](x: A) extends FetchResult[F, A]
  case class Blocked[F[_], A](rs: RequestMap[F], cont: Fetch[F, A]) extends FetchResult[F, A]
  case class Throw[F[_], A](e: Env[F] => FetchException[F]) extends FetchResult[F, A]

  // Fetch data type
  sealed trait Fetch[F[_], A] {
    def run: F[FetchResult[F, A]]
  }
  case class Unfetch[F[_], A](
    run: F[FetchResult[F, A]]
  ) extends Fetch[F, A]

  // Fetch ops
  implicit def fetchM[F[_]: Monad]: Monad[Fetch[F, ?]] = new Monad[Fetch[F, ?]] {
    def pure[A](a: A): Fetch[F, A] =
      Unfetch(
        Monad[F].pure(Done(a))
      )

    override def map[A, B](fa: Fetch[F, A])(f: A => B): Fetch[F, B] =
      Unfetch(for {
        fetch <- fa.run
        result = fetch match {
          case Done(v) => Done[F, B](f(v))
          case Blocked(br, cont) =>
            Blocked(br, map(cont)(f))
          case Throw(e) => Throw[F, B](e)
        }
      } yield result)

    override def product[A, B](fa: Fetch[F, A], fb: Fetch[F, B]): Fetch[F, (A, B)] =
      Unfetch[F, (A, B)](for {
        fab <- (fa.run, fb.run).tupled
        result = fab match {
          case (Throw(e), _) =>
            Throw[(A, B)](e)
          case (Done(a), Done(b)) =>
            Done((a, b))
          case (Done(a), Blocked(br, c)) =>
            Blocked(br, product(fa, c))
          case (Blocked(br, c), Done(b)) =>
            Blocked(br, product(c, fb))
          case (Blocked(br, c), Blocked(br2, c2)) =>
            Blocked(combineRequestMaps(br, br2), product(c, c2))
          case (_, Throw(e)) =>
            Throw[F, (A, B)](e)
        }
      } yield result)

    // todo: can be tail recursive?
    def tailRecM[A, B](a: A)(f: A => Fetch[Either[A, B]]): Fetch[B] =
      f(a).flatMap(_ match {
        case Left(a) => tailRecM(a)(f)
        case Right(b) => pure(b)
      })

    def flatMap[A, B](fa: Fetch[F, A])(f: A => Fetch[F, B]): Fetch[F, B] =
      Unfetch(for {
        fetch <- fa.run
        result: Fetch[F, B] = fetch match {
          case Done(v) => f(v)
          case Throw(e) => Unfetch[F, B](Monad[F].pure(Throw[F, B](e)))
          case Blocked(br, cont: Fetch[F, A]) =>
            Unfetch[F, B](
              Monad[F].pure(
                Blocked(br, flatMap(cont)(f))
              )
            )
        }
        value <- result.run
      } yield value)
  }

  object Fetch {
    /**
     * Lift a plain value to the Fetch monad.
     */
    def pure[F[_]: Monad, A](a: A): Fetch[F, A] =
      Unfetch(Monad[F].pure(Done(a)))

    def exception[F[_]: Monad, A](e: Env[F] => FetchException[F]): Fetch[F, A] =
      Unfetch(Monad[F].pure(Throw[F, A](e)))

    def error[F[_]: Monad, A](e: Throwable): Fetch[F, A] =
      exception((env) => UnhandledException(e, env))

    def apply[F[_]: Concurrent, I, A](id: I, ds: DataSource[F, I, A])(
      implicit
        CS: ContextShift[F]
    ): Fetch[F, A] =
      Unfetch[F, A](
        for {
          deferred <- Deferred[F, FetchStatus]
          request = FetchOne(id, ds)
          result = deferred.complete _
          blocked = BlockedRequest(request, result)
          anyDs = ds.asInstanceOf[DataSource[F, Any, Any]]
          blockedRequest = RequestMap(Map(anyDs -> blocked))
        } yield Blocked(blockedRequest, Unfetch[F, A](
          deferred.get.flatMap {
            case FetchDone(a: A) =>
              Monad[F].pure(Done(a))
            case FetchMissing() =>
              Monad[F].pure(Throw((env) => MissingIdentity(id, request, env)))
          }
        ))
      )

    /**
      * Run a `Fetch`, the result in the `IO` monad.
      */
    def run[F[_]: Sync: Par, A](
      fa: Fetch[F, A],
      cache: DataSourceCache[F] = InMemoryCache.empty[F]
    )(
      implicit
        CS: ContextShift[F],
        T: Timer[F]
    ): F[A] = for {
      cache <- Ref.of[F, DataSourceCache[F]](cache)
      result <- performRun(fa, cache, None)
    } yield result

    /**
      * Run a `Fetch`, the environment and the result in the `IO` monad.
      */
    def runEnv[F[_]: Sync: Par, A](
      fa: Fetch[F, A],
      cache: DataSourceCache[F] = InMemoryCache.empty[F]
    )(
      implicit
        CS: ContextShift[F],
        T: Timer[F]
    ): F[(Env[F], A)] = for {
      env <- Ref.of[F, Env[F]](FetchEnv())
      cache <- Ref.of[F, DataSourceCache[F]](cache)
      result <- performRun(fa, cache, Some(env))
      e <- env.get
    } yield (e, result)

    /**
      * Run a `Fetch`, the cache and the result in the `F` monad.
      */
    def runCache[F[_]: Sync: Par, A](
      fa: Fetch[F, A])(
      cache: DataSourceCache[F]
    )(
      implicit
        CS: ContextShift[F],
        T: Timer[F]
    ): F[(DataSourceCache[F], A)] = for {
      cache <- Ref.of[F, DataSourceCache[F]](cache)
      result <- performRun(fa, cache, None)
      c <- cache.get
    } yield (c, result)

    private def performRun[F[_]: Sync: Par, A](
      fa: Fetch[F, A],
      cache: Ref[F, DataSourceCache[F]],
      env: Option[Ref[F, Env[F]]]
    )(
      implicit
        CS: ContextShift[F],
        T: Timer[F]
    ): F[A] = for {
      result <- fa.run

      value <- result match {
        case Done(a) => Sync[F].pure(a)
        case Blocked(rs, cont) => for {
          _ <- fetchRound(rs, cache, env)
          result <- performRun(cont, cache, env)
        } yield result
        case Throw(envToThrowable) =>
          env.fold(Sync[F].pure(FetchEnv() : Env[F]))(_.get).flatMap((e: Env[F]) => Sync[F].raiseError(envToThrowable(e)))
      }
    } yield value

    private def fetchRound[F[_]: Sync: Par, A](
      rs: RequestMap[F],
      cache: Ref[F, DataSourceCache[F]],
      env: Option[Ref[F, Env[F]]]
    )(
      implicit
        CS: ContextShift[F],
        T: Timer[F]
    ): F[Unit] = {
      val blocked = rs.m.toList.map(_._2)
      if (blocked.isEmpty) Sync[F].unit
      else
        for {
          requests <- NonEmptyList.fromListUnsafe(blocked).parTraverse(
            runBlockedRequest(_, cache, env)
          )
          performedRequests = requests.foldLeft(List.empty[Request[F]])(_ ++ _)
          _ <- if (performedRequests.isEmpty) Sync[F].unit
          else env match {
            case Some(e) => e.modify((oldE) => (oldE.evolve(Round(performedRequests)), oldE))
            case None => IO.unit
          }
        } yield ()
    }

    private def runBlockedRequest[F[_]: Sync: Par, A](
      blocked: BlockedRequest[F],
      cache: Ref[F, DataSourceCache[F]],
      env: Option[Ref[F, Env[F]]]
    )(
      implicit
        CS: ContextShift[F],
        T: Timer[F]
    ): F[List[Request[F]]] =
      blocked.request match {
        case q @ FetchOne(id, ds) => runFetchOne[F](q, blocked.result, cache, env)
        case q @ Batch(ids, ds) => runBatch[F](q, blocked.result, cache, env)
      }
  }

  private def runFetchOne[F[_]: Sync](
    q: FetchOne[F, Any, Any],
    putResult: FetchStatus => F[Unit],
    cache: Ref[F, DataSourceCache[F]],
    env: Option[Ref[F, Env[F]]]
  )(
    implicit
      CS: ContextShift[F],
      T: Timer[F]
  ): F[List[Request[F]]] =
    for {
      c <- cache.get
      maybeCached <- c.lookup(q.id, q.ds)
      result <- maybeCached match {
        // Cached
        case Some(v) => putResult(FetchDone(v)) >> Sync[F].pure(Nil)

        // Not cached, must fetch
        case None => for {
          startTime <- T.clock.monotonic(MILLISECONDS)
          o <- q.ds.fetch(q.id)
          endTime <- T.clock.monotonic(MILLISECONDS)
          result <- o match {
            // Fetched
            case Some(a) => for {
              newC <- c.insert(q.id, a, q.ds)
              _ <- cache.set(newC)
              result <- putResult(FetchDone[Any](a))
            } yield List(Request(q, startTime, endTime))

            // Missing
            case None =>
              putResult(FetchMissing()) >> Sync[F].pure(List(Request(q, startTime, endTime)))
          }
        } yield result
      }
    } yield result

  private case class BatchedRequest[F[_]](
    batches: List[Batch[F, Any, Any]],
    results: Map[Any, Any]
  )

  private def runBatch[F[_]: Sync: Par](
    q: Batch[F, Any, Any],
    putResult: FetchStatus => F[Unit],
    cache: Ref[F, DataSourceCache[F]],
    env: Option[Ref[F, Env[F]]]
  )(
    implicit
      CS: ContextShift[F],
      T: Timer[F]
  ): F[List[Request[F]]] =
    for {
      c <- cache.get

      // Remove cached IDs
      idLookups <- q.ids.traverse[F, (Any, Option[Any])](
        (i) => c.lookup(i, q.ds).map( m => (i, m) )
      )
      cachedResults = idLookups.collect({
        case (i, Some(a)) => (i, a)
      }).toMap
      uncachedIds = idLookups.collect({
        case (i, None) => i
      })

      result <- uncachedIds match {
        // All cached
        case Nil => putResult(FetchDone[Map[Any, Any]](cachedResults)) >> Sync[F].pure(Nil)

        // Some uncached
        case l@_ => for {
          startTime <- T.clock.monotonic(MILLISECONDS)

          uncached = NonEmptyList.fromListUnsafe(l)
          request = Batch(uncached, q.ds)

          batchedRequest <- request.ds.maxBatchSize match {
            // Unbatched
            case None =>
              request.ds.batch(uncached).map(BatchedRequest(List(request), _))

            // Batched
            case Some(batchSize) =>
              runBatchedRequest(request, batchSize, request.ds.batchExecution)
          }

          endTime <- T.clock.monotonic(MILLISECONDS)
          resultMap = combineBatchResults(batchedRequest.results, cachedResults)

          updatedCache <- c.insertMany(batchedRequest.results, request.ds)
          _ <- cache.set(updatedCache)

          result <- putResult(FetchDone[Map[Any, Any]](resultMap))

        } yield batchedRequest.batches.map(Request(_, startTime, endTime))
      }
    } yield result

  private def runBatchedRequest[F[_]: Monad: Par](
    q: Batch[F, Any, Any],
    batchSize: Int,
    e: BatchExecution
  )(
    implicit
      CS: ContextShift[F],
      T: Timer[F]
  ): F[BatchedRequest[F]] = {
    val batches = NonEmptyList.fromListUnsafe(
      q.ids.toList.grouped(batchSize)
        .map(batchIds => NonEmptyList.fromListUnsafe(batchIds))
        .toList
    )
    val reqs = batches.toList.map(Batch[F, Any, Any](_, q.ds))

    val results = e match {
      case Sequentially =>
        batches.traverse(q.ds.batch)
      case InParallel =>
        batches.parTraverse(q.ds.batch)
    }

    results.map(_.toList.reduce(combineBatchResults)).map(BatchedRequest(reqs, _))
  }

  private def combineBatchResults(r: Map[Any, Any], rs: Map[Any, Any]): Map[Any, Any] =
    r ++ rs
}
