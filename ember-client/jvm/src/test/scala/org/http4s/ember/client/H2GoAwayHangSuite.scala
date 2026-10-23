/*
 * Copyright 2019 http4s.org
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

package org.http4s
package ember.client

import cats.effect._
import cats.syntax.all._
import org.http4s.ember.core.h2.GoAwayServer

import scala.concurrent.duration._

/** End-to-end reproduction through the public EmberClientBuilder API: after a peer
  * GOAWAY, requests started on the dead h2 connection cannot be cancelled.
  */
class H2GoAwayHangSuite extends Http4sSuite {

  /** Returns whether cancelling `n` requests started on the dead connection completed. */
  private def cancelAfterGoAway(n: Int): IO[Boolean] =
    (GoAwayServer.resource, EmberClientBuilder.default[IO].withHttp2.build).tupled.use {
      case (port, client) =>
        val req = GoAwayServer.request(port)
        for {
          // 1. The in-flight request fails with the GOAWAY: in a ScalaCheck property this
          //    is the error that makes the runner cancel the other in-flight runs.
          first <- client.expect[String](req).attempt
          _ = assert(first.left.exists(_.getMessage.contains("Received GoAway")), clue(first))
          // 2. Further requests reuse the dead connection: it stays in the H2Client map
          //    until the 1s reaper notices `closed`.
          fibers <- List.fill(n)(client.expect[String](req)).traverse(_.start)
          _ <- IO.sleep(2.seconds)
          // 3. Cancel them, as ScalaCheck / a test timeout would.
          cancelled <- fibers
            .traverse_(_.cancel)
            .start
            .flatMap(_.join.as(true).timeoutTo(5.seconds, IO.pure(false)))
        } yield cancelled
    }

  test("control: fewer requests than the queue capacity can be cancelled") {
    cancelAfterGoAway(20).assertEquals(true)
  }

  test("requests on a torn-down h2 connection cannot be cancelled") {
    cancelAfterGoAway(300).assertEquals(false, "cancellation should hang (bug reproduced)")
  }
}
